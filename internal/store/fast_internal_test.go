package store

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestParamsEncoding(t *testing.T) {
	p := getParams()
	defer p.release()
	p.text("")
	p.text("abc")
	p.json(nil)
	p.json([]byte(`{"a":1}`))
	p.int(-42)
	p.int(1 << 40)
	p.bool(true)
	p.bool(false)

	want := []struct {
		null bool
		v    string
	}{{v: ""}, {v: "abc"}, {null: true}, {v: `{"a":1}`}, {v: "-42"}, {v: "1099511627776"}, {v: "t"}, {v: "f"}}
	if len(p.vals) != len(want) {
		t.Fatalf("got %d values, want %d", len(p.vals), len(want))
	}
	for i, w := range want {
		got := p.vals[i]
		if (got == nil) != w.null || string(got) != w.v {
			t.Errorf("value %d = %q (nil=%v), want %q (null=%v)", i, got, got == nil, w.v, w.null)
		}
	}
}

// Integers formatted after the scratch buffer grew must not overwrite the
// ones formatted before.
func TestParamsIntGrowth(t *testing.T) {
	p := &params{num: make([]byte, 0, 2)}
	for i := range 50 {
		p.int(int64(1000 + i))
	}
	for i, v := range p.vals {
		if want := 1000 + i; string(v) != strconv.Itoa(want) {
			t.Fatalf("value %d = %q, want %d", i, v, want)
		}
	}
}

func TestDecodeInt(t *testing.T) {
	b2 := binary.BigEndian.AppendUint16(nil, 0xfffe)
	b4 := binary.BigEndian.AppendUint32(nil, 7)
	b8 := binary.BigEndian.AppendUint64(nil, 1<<40)
	for _, c := range []struct {
		b    []byte
		want int64
	}{{b2, -2}, {b4, 7}, {b8, 1 << 40}} {
		got, err := decodeInt(c.b)
		if err != nil || got != c.want {
			t.Errorf("decodeInt(%x) = %d, %v; want %d", c.b, got, err, c.want)
		}
	}
	if _, err := decodeInt([]byte{1, 2, 3}); err == nil {
		t.Error("decodeInt of 3 bytes: no error")
	}
}

func TestDecodeTimestamptz(t *testing.T) {
	for _, want := range []time.Time{
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 12, 10, 20, 30, 123456000, time.UTC),
		time.Date(1999, 12, 31, 23, 59, 59, 999999000, time.UTC), // negative, sub-second
		time.Date(1970, 1, 1, 0, 0, 0, 1000, time.UTC),
	} {
		us := want.Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Microseconds()
		got, err := decodeTimestamptz(binary.BigEndian.AppendUint64(nil, uint64(us)))
		if err != nil || !got.Equal(want) {
			t.Errorf("decode %v: got %v, %v", want, got, err)
		}
	}
	if _, err := decodeTimestamptz(binary.BigEndian.AppendUint64(nil, 1<<63-1)); err == nil {
		t.Error("infinity: no error")
	}
	if _, err := decodeTimestamptz([]byte{1}); err == nil {
		t.Error("short value: no error")
	}
}

type nopTracer struct{}

func (nopTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (nopTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// The fast path bypasses pgx's tracer and statement modes: it must only be
// used when neither matters.
func TestFastPathEnabled(t *testing.T) {
	for _, c := range []struct {
		name   string
		mutate func(*pgxpool.Config)
		want   bool
	}{
		{"default", func(*pgxpool.Config) {}, true},
		{"tracer", func(c *pgxpool.Config) { c.ConnConfig.Tracer = nopTracer{} }, false},
		{"exec", func(c *pgxpool.Config) { c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec }, false},
		{"describe", func(c *pgxpool.Config) { c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe }, false},
		{"simple", func(c *pgxpool.Config) { c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol }, false},
	} {
		cfg, err := pgxpool.ParseConfig("postgres://u@127.0.0.1:1/db")
		if err != nil {
			t.Fatal(err)
		}
		c.mutate(cfg)
		pool, err := pgxpool.NewWithConfig(context.Background(), cfg) // connects lazily
		if err != nil {
			t.Fatal(err)
		}
		if got := fastPathEnabled(pool); got != c.want {
			t.Errorf("%s: fastPathEnabled = %v, want %v", c.name, got, c.want)
		}
		pool.Close()
	}
}

// Interrupt bookkeeping: fire sets a past deadline only on the connection of
// the statement in flight, and no statement starts after it fired.
func TestInterruptBookkeeping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := (&Store{fast: true}).NewInterrupt(ctx)
	defer in.Stop()
	if !in.covers(ctx) || in.covers(context.Background()) {
		t.Fatal("covers: wrong context match")
	}
	narrow, cancelNarrow := context.WithTimeout(ctx, time.Hour)
	defer cancelNarrow()
	if in.covers(narrow) {
		t.Fatal("covers a narrower context")
	}

	idle, idlePeer := net.Pipe()
	defer idle.Close()
	defer idlePeer.Close()
	busy, busyPeer := net.Pipe()
	defer busy.Close()
	defer busyPeer.Close()

	// A statement that ended before the cancellation is not touched.
	if !in.begin(idle) || in.end() {
		t.Fatal("begin/end before cancellation")
	}
	if !in.begin(busy) {
		t.Fatal("begin refused before cancellation")
	}
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		in.mu.Lock()
		fired := in.fired
		in.mu.Unlock()
		if fired || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !in.end() {
		t.Fatal("end did not report the cancellation")
	}
	if in.begin(idle) {
		t.Fatal("begin accepted after cancellation")
	}
	// fire set a past deadline on busy, never on idle.
	if _, err := busy.Write([]byte("x")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("busy conn write: %v, want deadline exceeded", err)
	}
	go func() { _, _ = idlePeer.Read(make([]byte, 1)) }()
	if _, err := idle.Write([]byte("x")); err != nil {
		t.Fatalf("idle conn got a deadline: %v", err)
	}
}
