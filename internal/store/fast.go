package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The fast path runs the hot-path statements (step checkpoints, run insert
// and finish) on pgconn directly, below pgx's Query/Scan layer.
//
// Why: pgx boxes every argument into an interface (one allocation per string,
// slice or large integer), allocates a rows object and two scan-plan slices
// per query, and plans each scan. With ~22 statements per 10-step run that was
// ~330 of the ~500 allocations of BenchmarkStepRun. Here parameters are sent
// in text format from a pooled buffer (strings are passed without copying,
// integers are formatted into a scratch slice), results come back in binary
// format and are decoded by hand. Context watching is per execution, not per
// statement (see Interrupt). What remains per statement is the command tag
// pgconn copies into a string (1 allocation).
//
// Statements are prepared once per connection under a fixed name, like pgx's
// default statement cache does. The fast path is therefore only used when the
// pool runs in QueryExecModeCacheStatement (the default; other modes are how
// PgBouncer in transaction mode is usually configured), and when no query
// tracer is set (it would not see these statements). Otherwise the same SQL
// goes through the regular pgx path.

// fastStmt is a hot-path statement with its per-connection prepared name.
// Bump the name suffix when the SQL changes, so a name always maps to one
// text (prepared statements live per connection, within one process).
type fastStmt struct {
	name string
	sql  string
}

// fastPathEnabled reports whether pool's configuration allows the fast path.
func fastPathEnabled(pool *pgxpool.Pool) bool {
	cc := pool.Config().ConnConfig
	return cc.DefaultQueryExecMode == pgx.QueryExecModeCacheStatement && cc.Tracer == nil
}

// resultBinary requests every result column in binary format.
var resultBinary = []int16{1}

// params is a reusable parameter list in text format. Values reference the
// caller's strings and byte slices without copying: they must stay unchanged
// until the statement has been sent, which pgconn does before returning.
type params struct {
	vals [][]byte
	num  []byte // backing store of formatted integers
}

// maxParamsNum bounds the scratch kept by a pooled params, so one odd call
// cannot pin a large buffer for the process lifetime.
const maxParamsNum = 1024

var paramsPool = sync.Pool{New: func() any {
	return &params{vals: make([][]byte, 0, 12), num: make([]byte, 0, 128)}
}}

func getParams() *params {
	if p, ok := paramsPool.Get().(*params); ok {
		return p
	}
	return &params{}
}

// release clears the references to the caller's data and returns p to the
// pool.
func (p *params) release() {
	clear(p.vals)
	p.vals = p.vals[:0]
	if cap(p.num) > maxParamsNum {
		return
	}
	p.num = p.num[:0]
	paramsPool.Put(p)
}

var (
	textEmpty = []byte{} // non-nil: an empty string, not NULL
	textTrue  = []byte("t")
	textFalse = []byte("f")
)

// text appends a text parameter. s is sent as is: pgconn only reads it.
func (p *params) text(s string) {
	if s == "" {
		p.vals = append(p.vals, textEmpty)
		return
	}
	p.vals = append(p.vals, unsafe.Slice(unsafe.StringData(s), len(s))) //nolint:gosec // G103: read-only view, see params
}

// json appends a json/jsonb parameter; empty means NULL (the engine never
// passes an empty non-nil document: it holds encoded JSON or nil).
func (p *params) json(b []byte) {
	if len(b) == 0 {
		p.vals = append(p.vals, nil)
		return
	}
	p.vals = append(p.vals, b)
}

func (p *params) int(n int64) {
	start := len(p.num)
	p.num = strconv.AppendInt(p.num, n, 10)
	p.vals = append(p.vals, p.num[start:len(p.num):len(p.num)])
}

func (p *params) bool(b bool) {
	if b {
		p.vals = append(p.vals, textTrue)
		return
	}
	p.vals = append(p.vals, textFalse)
}

// Interrupt stops the fast-path statement in flight of one run execution
// when the execution is cancelled, in place of pgconn's per-statement watch.
//
// Why: pgconn watches a cancellable context with context.AfterFunc on every
// statement (4 allocations each; a run context is always cancellable). An
// Interrupt is a plain value embedded in the caller's per-execution state
// (the heartbeat entry of the run): no allocation of its own, no watch of its
// own. Whoever cancels a covered context must call Fire right after: the
// heartbeat does when a lease is lost or on shutdown, and an AfterFunc on the
// caller's context does when that one is cancelled. Fire sets a past deadline
// on the connection of the statement in flight, which is what pgconn's
// default handler (DeadlineContextWatcherHandler) does: the read fails and
// pgconn closes the connection. A user-configured pgconn
// BuildContextWatcherHandler (such as the cancel-request handler) is bypassed
// for these statements. Covered statements run with context.Background(), so
// pgconn does not watch them; begin also refuses to start one once the
// covered context is done, should a cancellation path forget to Fire.
//
// The statements of one execution are sequential (one goroutine): one
// connection at a time is tracked; begin/end and Fire are serialised by mu.
// Cover is called by that goroutine only, before its statements.
type Interrupt struct {
	covered [2]context.Context // statements under these contexts rely on Fire

	mu    sync.Mutex
	conn  net.Conn // connection of the statement in flight, nil between statements
	fired bool
}

// Cover makes statements issued under ctx (compared by identity) rely on in
// rather than on pgconn's watch. ctx must be cancelled only by a path that
// calls Fire. At most two contexts; further calls are ignored (their
// statements keep pgconn's watch).
func (in *Interrupt) Cover(ctx context.Context) {
	if in == nil {
		return
	}
	switch {
	case in.covered[0] == nil:
		in.covered[0] = ctx //nolint:fatcontext // stored for identity comparison, not wrapped
	case in.covered[1] == nil:
		in.covered[1] = ctx
	}
}

// Fire interrupts the statement in flight, if any, and every later one.
// Safe on nil and from any goroutine.
func (in *Interrupt) Fire() {
	if in == nil {
		return
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	in.fired = true
	if in.conn != nil {
		_ = in.conn.SetDeadline(time.Now())
	}
}

// Covers reports whether in covers any context.
func (in *Interrupt) Covers() bool { return in != nil && in.covered[0] != nil }

// covers reports whether a statement under ctx may rely on in. Contexts
// are pointers in practice, so the comparison cannot panic.
func (in *Interrupt) covers(ctx context.Context) bool {
	return in != nil && ctx != nil && (ctx == in.covered[0] || ctx == in.covered[1])
}

// begin tracks conn for a statement under ctx; false if the execution was
// interrupted or ctx is already done.
func (in *Interrupt) begin(ctx context.Context, conn net.Conn) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.fired || ctx.Err() != nil {
		return false
	}
	in.conn = conn
	return true
}

// end stops tracking and reports whether the execution was interrupted
// meanwhile.
func (in *Interrupt) end() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.conn = nil
	return in.fired
}

// interrupted is the error of a statement stopped by its context. Fire may
// run just before the context reports done (the caller's cancellation
// reaches it through an AfterFunc): context.Canceled stands in meanwhile.
func interrupted(ctx context.Context, err error) error {
	cause := ctx.Err()
	if cause == nil {
		cause = context.Canceled
	}
	if err == nil {
		return cause
	}
	return fmt.Errorf("%w: %w", cause, err)
}

// query runs st with p on a pooled connection, calling row for each result
// row (binary values, valid during the call only). It returns how many rows
// the statement returned. in (may be nil) replaces pgconn's context watch
// when it covers ctx.
func (s *Store) query(ctx context.Context, in *Interrupt, st *fastStmt, p *params, row func(vals [][]byte) error) (int, error) {
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()
	pc := c.Conn().PgConn()
	if !in.covers(ctx) {
		return s.exec(ctx, c.Conn(), st, p, row)
	}
	if !in.begin(ctx, pc.Conn()) {
		return 0, interrupted(ctx, nil)
	}
	// Also deferred (end is idempotent), so that a panic in a row callback
	// cannot leave the pooled connection tracked, where a late Fire would
	// hit another user's statement. Runs before Release.
	defer in.end()
	n, err := s.exec(context.Background(), c.Conn(), st, p, row) // no watch: in stands in for it
	if in.end() {
		// Cancelled during the statement: the deadline set by Fire must not
		// follow the connection back to the pool (pgconn resets it the
		// same way after its own watch fired). If the statement completed
		// anyway, its result stands, as with pgconn.
		// Skip it once pgconn closed the connection: its cleanup sets its
		// own deadline to bound draining the socket.
		if !pc.IsClosed() {
			_ = pc.Conn().SetDeadline(time.Time{})
		}
		if err != nil {
			err = interrupted(ctx, err)
		}
	}
	return n, err
}

func (s *Store) exec(ctx context.Context, conn *pgx.Conn, st *fastStmt, p *params, row func(vals [][]byte) error) (int, error) {
	sd, err := conn.Prepare(ctx, st.name, st.sql)
	if err != nil {
		return 0, err
	}
	rr := conn.PgConn().ExecStatement(ctx, sd, p.vals, nil, resultBinary)
	n := 0
	var rowErr error
	for rr.NextRow() {
		n++
		if rowErr == nil && row != nil {
			rowErr = rowErrorf(row(rr.Values()))
		}
	}
	if _, err := rr.Close(); err != nil {
		if staleStatement(err) {
			// The prepared statement no longer matches the schema (or is
			// gone): drop the connection, the next one prepares afresh.
			_ = conn.Close(ctx)
		}
		return n, err
	}
	return n, rowErr
}

func rowErrorf(err error) error {
	if err != nil {
		return fmt.Errorf("decode row: %w", err)
	}
	return nil
}

// staleStatement reports errors after which a prepared statement must not be
// reused: "cached plan must not change result type" (0A000, a class that also
// holds other "feature not supported" errors: those only cost a reconnect) and
// "prepared statement does not exist" (26000).
func staleStatement(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "0A000" || pgErr.Code == "26000")
}

var errBadValue = errors.New("unexpected binary value")

// decodeInt decodes a binary int2, int4 or int8.
func decodeInt(b []byte) (int64, error) {
	switch len(b) {
	case 2:
		return int64(int16(binary.BigEndian.Uint16(b))), nil
	case 4:
		return int64(int32(binary.BigEndian.Uint32(b))), nil
	case 8:
		return int64(binary.BigEndian.Uint64(b)), nil
	}
	return 0, fmt.Errorf("%w: integer of %d bytes", errBadValue, len(b))
}

// pgEpochUnix is 2000-01-01 00:00:00 UTC, the epoch of binary timestamps.
const pgEpochUnix = 946684800

// decodeTimestamptz decodes a binary timestamptz (microseconds since
// 2000-01-01 UTC), in the local time zone like pgx does.
func decodeTimestamptz(b []byte) (time.Time, error) {
	if len(b) != 8 {
		return time.Time{}, fmt.Errorf("%w: timestamptz of %d bytes", errBadValue, len(b))
	}
	us := int64(binary.BigEndian.Uint64(b))
	if us == math.MaxInt64 || us == math.MinInt64 {
		return time.Time{}, fmt.Errorf("%w: infinite timestamptz", errBadValue)
	}
	sec, rem := us/1e6, us%1e6
	if rem < 0 {
		sec--
		rem += 1e6
	}
	return time.Unix(pgEpochUnix+sec, rem*1e3), nil
}
