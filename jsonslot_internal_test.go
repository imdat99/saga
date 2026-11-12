package saga

import (
	"bytes"
	"encoding/json"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type ptrMarshal struct{ A int }

func (p *ptrMarshal) MarshalJSON() ([]byte, error) { return []byte(`"ptr"`), nil }

type ptrText struct{ A int }

func (p *ptrText) MarshalText() ([]byte, error) { return []byte("ptrtext"), nil }

type valMarshal struct{ A int }

type ptrIsZero struct{ A int }

func (p *ptrIsZero) IsZero() bool { return true }

type ptrTextKey string

func (k *ptrTextKey) MarshalText() ([]byte, error) { return []byte("key"), nil }

type withOmitZero struct {
	Z ptrIsZero `json:",omitzero"`
	N int
}

func (valMarshal) MarshalJSON() ([]byte, error) { return []byte(`"val"`), nil }

type (
	plain struct {
		ID   string
		N    int
		Tags []string
		M    map[string]int
		T    time.Time
	}
	nestedPtrM struct{ X ptrMarshal }
	arrayPtrM  struct{ X [2]ptrText }
	embedPtrM  struct {
		ptrMarshal
		B int
	}
	behindPtr   struct{ X *ptrMarshal }
	behindSlice struct{ X []ptrMarshal }
	behindMap   struct{ X map[string]ptrMarshal }
	behindIface struct{ X any }
	withValM    struct{ X valMarshal }
	recursive   struct {
		Next *recursive
		N    int
	}
	recursiveArr struct {
		Kids []recursiveArr
		V    int
	}
	withBigInt struct{ X big.Int }
	withRaw    struct{ X json.RawMessage }
)

func TestAddrIndependent(t *testing.T) {
	for _, c := range []struct {
		t    reflect.Type
		want bool
	}{
		{reflect.TypeFor[int](), true},
		{reflect.TypeFor[string](), true},
		{reflect.TypeFor[struct{}](), true},
		{reflect.TypeFor[plain](), true},
		{reflect.TypeFor[*plain](), true},
		{reflect.TypeFor[[]ptrMarshal](), true},
		{reflect.TypeFor[map[string]ptrMarshal](), true},
		{reflect.TypeFor[behindPtr](), true},
		{reflect.TypeFor[behindSlice](), true},
		{reflect.TypeFor[behindMap](), true},
		{reflect.TypeFor[behindIface](), true},
		{reflect.TypeFor[withValM](), true},
		{reflect.TypeFor[recursive](), true},
		{reflect.TypeFor[recursiveArr](), true},
		{reflect.TypeFor[withRaw](), true},
		{reflect.TypeFor[any](), false},
		{reflect.TypeFor[ptrMarshal](), false},
		{reflect.TypeFor[ptrText](), false},
		{reflect.TypeFor[nestedPtrM](), false},
		{reflect.TypeFor[arrayPtrM](), false},
		{reflect.TypeFor[embedPtrM](), false},
		{reflect.TypeFor[[3]ptrMarshal](), false},
		{reflect.TypeFor[big.Int](), false},
		{reflect.TypeFor[withBigInt](), false},
	} {
		if got := addrIndependent(c.t); got != c.want {
			t.Errorf("addrIndependent(%v) = %v, want %v", c.t, got, c.want)
		}
	}
}

// checkSame encodes v through a slot both ways (appendTo after existing
// content, marshal) and compares with json.Marshal.
func checkSame[T any](t *testing.T, v T) {
	t.Helper()
	want, wantErr := json.Marshal(v)
	slot := newJSONSlot[T]()
	got, err := slot.marshal(v)
	if (err == nil) != (wantErr == nil) || !bytes.Equal(got, want) {
		t.Errorf("%T marshal: %s (%v), want %s (%v)", v, got, err, want, wantErr)
	}
	var buf bytes.Buffer
	buf.WriteString("prefix")
	err = slot.appendTo(&buf, v)
	if (err == nil) != (wantErr == nil) || (err == nil && buf.String() != "prefix"+string(want)) {
		t.Errorf("%T appendTo: %s (%v), want prefix%s (%v)", v, buf.String(), err, want, wantErr)
	}
}

func TestJSONSlotSameBytes(t *testing.T) {
	checkSame(t, 7)
	checkSame(t, 123456)
	checkSame(t, "a<b>&c \xff")
	checkSame(t, struct{}{})
	checkSame(t, plain{ID: "<id>", N: 3, T: time.Unix(1, 2).UTC()})
	checkSame(t, plain{Tags: []string{}, M: map[string]int{"z": 1, "a": 2}})
	checkSame(t, &plain{ID: "p"})
	checkSame(t, (*plain)(nil))
	checkSame(t, ptrMarshal{1})
	checkSame(t, ptrText{1})
	checkSame(t, nestedPtrM{})
	checkSame(t, arrayPtrM{})
	checkSame(t, embedPtrM{B: 2})
	checkSame(t, behindPtr{X: &ptrMarshal{}})
	checkSame(t, behindSlice{X: []ptrMarshal{{1}}})
	checkSame(t, behindMap{X: map[string]ptrMarshal{"k": {1}}})
	checkSame(t, behindIface{X: ptrMarshal{1}})
	checkSame(t, behindIface{X: &ptrMarshal{1}})
	checkSame(t, withValM{})
	checkSame(t, recursive{Next: &recursive{N: 2}, N: 1})
	checkSame(t, withBigInt{})
	checkSame(t, *big.NewInt(42))
	checkSame(t, withRaw{X: json.RawMessage(`{"a":1}`)})
	checkSame(t, any(ptrMarshal{1}))
	checkSame(t, []any{1, "x", nil})
	checkSame(t, map[string]any{"ch": make(chan int)}) // unsupported: both fail
	checkSame(t, strings.Repeat("x", 5000))
	checkSame(t, &ptrMarshal{1}) // pointer at the root
	checkSame(t, (*ptrMarshal)(nil))
	checkSame(t, withOmitZero{N: 1})                     // IsZero on a pointer receiver
	checkSame(t, map[ptrTextKey]int{"a": 1})             // MarshalText on a pointer key
	checkSame(t, struct{ M map[ptrTextKey]int }{M: nil}) // nil map field
}

func TestJSONSlotAllocs(t *testing.T) {
	type out struct {
		ID    string
		Count int
	}
	if raceEnabled {
		t.Skip("sync.Pool drops items at random under the race detector")
	}
	slot := newJSONSlot[out]()
	var buf bytes.Buffer
	v := out{ID: "ord_123", Count: 1 << 20}
	_ = slot.appendTo(&buf, v) // warm the pool and buffer
	if n := testing.AllocsPerRun(100, func() {
		buf.Reset()
		_ = slot.appendTo(&buf, v)
	}); n != 0 {
		t.Errorf("appendTo allocates %v times, want 0", n)
	}
}

func TestNewRunID(t *testing.T) {
	prev := ""
	before := time.Now().Add(-time.Second)
	for range 20000 {
		id := newRunID()
		u, err := uuid.Parse(id)
		if err != nil || u.String() != id {
			t.Fatalf("%q: %v", id, err)
		}
		if u.Version() != 7 || u.Variant() != uuid.RFC4122 {
			t.Fatalf("%q: version %d variant %v", id, u.Version(), u.Variant())
		}
		if id <= prev {
			t.Fatalf("not monotonic: %q after %q", id, prev)
		}
		prev = id
		sec, nsec := u.Time().UnixTime()
		if ts := time.Unix(sec, nsec); ts.Before(before) || ts.After(time.Now().Add(time.Second)) {
			t.Fatalf("%q: time %v", id, ts)
		}
	}
	if raceEnabled {
		return // the race detector adds allocations
	}
	if n := testing.AllocsPerRun(100, func() { _ = newRunID() }); n != 1 {
		t.Errorf("newRunID allocates %v times, want 1 (the string)", n)
	}
}
