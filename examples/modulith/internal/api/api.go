// Package api exposes the flows over HTTP.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/examples/modulith/internal/flows"
	"github.com/imdat99/saga/examples/modulith/internal/modules/inventory"
)

// RunTimeout bounds how long a request waits for its run before answering
// 202; the run itself keeps going in the background.
const RunTimeout = 30 * time.Second

// maxIdempotencyKey keeps keys well under the btree index entry limit.
const maxIdempotencyKey = 200

// MaxInline bounds the checkouts executed inline by one instance (they are
// not counted by the engine's MaxWorkers). Beyond it, checkouts are enqueued
// with Workflow.Start and run on the queue, which has its own bound.
const MaxInline = 256

// Server serves the HTTP API.
type Server struct {
	inline chan struct{} // semaphore of inline runs
	eng    *saga.Engine
	flows  *flows.Flows
	inv    *inventory.Service
	log    *slog.Logger
}

// New returns the API server.
func New(eng *saga.Engine, f *flows.Flows, inv *inventory.Service, log *slog.Logger) *Server {
	return &Server{inline: make(chan struct{}, MaxInline), eng: eng, flows: f, inv: inv, log: log}
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.completeOrder)
	mux.HandleFunc("GET /runs/{id}", s.runStatus)
	mux.HandleFunc("POST /runs/{id}/retry-compensation", s.retryCompensation)
	mux.HandleFunc("GET /stock/{sku}", s.stock)
	return mux
}

type runAccepted struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type problem struct {
	Error  string `json:"error"`
	RunID  string `json:"run_id,omitempty"`
	Status string `json:"status,omitempty"`
}

func (s *Server) completeOrder(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > maxIdempotencyKey {
		writeJSON(w, http.StatusBadRequest, problem{Error: "Idempotency-Key header required (at most 200 bytes)"})
		return
	}
	var in flows.CompleteOrderInput
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, problem{Error: err.Error()})
		return
	}
	if in.CartID == "" || in.SKU == "" || in.Qty <= 0 || in.Amount <= 0 {
		writeJSON(w, http.StatusBadRequest, problem{Error: "cart_id, sku, qty > 0 and amount > 0 are required"})
		return
	}
	out, err := s.run(r.Context(), "order:"+key, in)
	if err == nil {
		writeJSON(w, http.StatusCreated, out)
		return
	}
	var re *saga.RunError
	switch {
	case errors.As(err, &re) && re.Status == saga.StatusCompensated:
		writeJSON(w, http.StatusUnprocessableEntity, problem{Error: re.Cause, RunID: re.RunID, Status: string(re.Status)})
	case errors.As(err, &re):
		s.log.Error("run needs attention", "run", re.RunID, "cause", re.Cause)
		writeJSON(w, http.StatusInternalServerError, problem{Error: "order needs attention", RunID: re.RunID, Status: string(re.Status)})
	case errors.Is(err, saga.ErrRunWaiting),
		errors.Is(err, saga.ErrRunInProgress),
		errors.Is(err, context.DeadlineExceeded):
		h, lerr := s.flows.CompleteOrder.Lookup(r.Context(), s.eng, "order:"+key)
		if lerr != nil {
			writeJSON(w, http.StatusServiceUnavailable, problem{Error: err.Error()})
			return
		}
		w.Header().Set("Location", "/runs/"+h.ID())
		writeJSON(w, http.StatusAccepted, runAccepted{RunID: h.ID(), Status: "pending"})
	case errors.Is(err, saga.ErrIdemKeyConflict):
		writeJSON(w, http.StatusConflict, problem{Error: err.Error()})
	default:
		s.log.Error("complete order", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, problem{Error: "try again"})
	}
}

type outcome struct {
	out flows.CompleteOrderResult
	err error
}

// run executes the checkout inline, detached from the request: the client
// going away or RunTimeout elapsing must not interrupt it (an interrupted run
// would wait for its lease to expire before anyone resumes it). After
// RunTimeout the handler answers and the run finishes in the background;
// Engine.Stop hands it over to another instance on shutdown.
func (s *Server) run(ctx context.Context, key string, in flows.CompleteOrderInput) (flows.CompleteOrderResult, error) {
	var wait <-chan outcome
	select {
	case s.inline <- struct{}{}:
		done := make(chan outcome, 1)
		go func() {
			defer func() { <-s.inline }()
			out, err := s.flows.CompleteOrder.Run(context.WithoutCancel(ctx), s.eng, key, in)
			done <- outcome{out: out, err: err}
		}()
		wait = done
	default:
		// Saturated: let the queue (bounded by MaxWorkers) run it.
		if _, err := s.flows.CompleteOrder.Start(ctx, s.eng, key, in); err != nil {
			return flows.CompleteOrderResult{}, err
		}
		return flows.CompleteOrderResult{}, saga.ErrRunWaiting
	}
	t := time.NewTimer(RunTimeout)
	defer t.Stop()
	select {
	case o := <-wait:
		return o.out, o.err
	case <-t.C:
		return flows.CompleteOrderResult{}, context.DeadlineExceeded
	case <-ctx.Done(): // client gone; the run goes on
		return flows.CompleteOrderResult{}, ctx.Err()
	}
}

func (s *Server) runStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.flows.CompleteOrder.Handle(s.eng, r.PathValue("id")).Status(r.Context())
	if errors.Is(err, saga.ErrRunNotFound) {
		writeJSON(w, http.StatusNotFound, problem{Error: err.Error()})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, problem{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, runAccepted{RunID: r.PathValue("id"), Status: string(st)})
}

func (s *Server) retryCompensation(w http.ResponseWriter, r *http.Request) {
	err := s.eng.RetryCompensation(r.Context(), r.PathValue("id"))
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, runAccepted{RunID: r.PathValue("id"), Status: string(saga.StatusCompensating)})
	case errors.Is(err, saga.ErrRunNotFound):
		writeJSON(w, http.StatusNotFound, problem{Error: err.Error()})
	case errors.Is(err, saga.ErrNotNeedsAttention):
		writeJSON(w, http.StatusConflict, problem{Error: err.Error()})
	default:
		writeJSON(w, http.StatusServiceUnavailable, problem{Error: err.Error()})
	}
}

func (s *Server) stock(w http.ResponseWriter, r *http.Request) {
	n, err := s.inv.Available(r.Context(), r.PathValue("sku"))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		writeJSON(w, http.StatusNotFound, problem{Error: "unknown sku"})
		return
	case err != nil:
		writeJSON(w, http.StatusServiceUnavailable, problem{Error: "try again"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"available": n})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
