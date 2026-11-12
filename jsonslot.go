package saga

import (
	"bytes"
	"encoding"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"reflect"
	"sync"
)

// jsonSlot encodes values of T through a pooled *T when that provably gives
// the same bytes as encoding the value.
//
// Why: encoding a non-pointer value boxes it into an interface (an
// allocation unless it is pointer-shaped or a small integer) and makes the
// json package copy it into a fresh addressable value (reflect.New, another
// allocation). Through a pointer taken from a pool, neither happens.
//
// When it is the same: under encoding/json's (v1) rules, methods with a
// pointer receiver (MarshalJSON, MarshalText, ...) are only called on
// addressable values. Encoding &v makes v, and everything reached from v
// through struct fields and array elements, addressable; values reached
// through pointers, slices, maps and interfaces are equally addressable (or
// not) either way. So the bytes can differ only if one of those types has
// such a method on its pointer and not on its value: then the value is
// encoded as before.
type jsonSlot[T any] struct {
	byPtr bool
	pool  sync.Pool // of *T, zeroed
}

func newJSONSlot[T any]() *jsonSlot[T] {
	return &jsonSlot[T]{byPtr: addrIndependent(reflect.TypeFor[T]())}
}

// appendTo encodes v at the end of buf, exactly as json.Marshal would.
func (s *jsonSlot[T]) appendTo(buf *bytes.Buffer, v T) error {
	if !s.byPtr {
		return jsonv2.MarshalWrite(buf, v, jsonV1)
	}
	p := s.get(v)
	defer s.put(p)
	return jsonv2.MarshalWrite(buf, p, jsonV1)
}

// marshal is json.Marshal(v).
func (s *jsonSlot[T]) marshal(v T) ([]byte, error) {
	if !s.byPtr {
		return json.Marshal(v)
	}
	p := s.get(v)
	defer s.put(p)
	return jsonv2.Marshal(p, jsonV1)
}

func (s *jsonSlot[T]) get(v T) *T {
	p, _ := s.pool.Get().(*T)
	if p == nil {
		p = new(T)
	}
	*p = v
	return p
}

// put zeroes p (no reference to the caller's data stays in the pool).
func (s *jsonSlot[T]) put(p *T) {
	var zero T
	*p = zero
	s.pool.Put(p)
}

var marshalIfaces = []reflect.Type{
	reflect.TypeFor[json.Marshaler](),
	reflect.TypeFor[jsonv2.MarshalerTo](),
	reflect.TypeFor[encoding.TextMarshaler](),
	reflect.TypeFor[encoding.TextAppender](),
}

// addrIndependent reports whether encoding a value of type t gives the same
// bytes as encoding a pointer to it (see jsonSlot). Interfaces are left
// alone (encoded by value) to stay on the safe side.
func addrIndependent(t reflect.Type) bool {
	if t.Kind() == reflect.Interface {
		return false
	}
	return addrWalk(t, map[reflect.Type]bool{})
}

func addrWalk(t reflect.Type, seen map[reflect.Type]bool) bool {
	if seen[t] {
		return true
	}
	seen[t] = true
	if t.Kind() != reflect.Pointer && t.Kind() != reflect.Interface {
		pt := reflect.PointerTo(t)
		for _, i := range marshalIfaces {
			if pt.Implements(i) && !t.Implements(i) {
				return false
			}
		}
	}
	switch t.Kind() {
	case reflect.Struct:
		for f := range t.Fields() { // unexported fields too: embedded ones are encoded
			if !addrWalk(f.Type, seen) {
				return false
			}
		}
	case reflect.Array:
		return addrWalk(t.Elem(), seen)
	default:
	}
	return true
}
