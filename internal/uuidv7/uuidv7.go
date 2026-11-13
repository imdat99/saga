// Package uuidv7 generates UUIDv7 strings without the allocations of
// github.com/google/uuid: run ids and event ids are made on hot paths.
package uuidv7

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// New returns a UUIDv7 string, monotonic within the process, like
// uuid.NewV7().String() (same layout: 48-bit Unix milliseconds, 12 bits of
// sub-millisecond sequence, 62 random bits). Generated here because
// uuid.NewV7 reads its random bytes through an io.Reader, which costs an
// allocation per run; crypto/rand.Read into a stack array does not.
func New() string {
	milli, seq := v7Clock()
	var u [16]byte
	_, _ = rand.Read(u[8:]) // never fails (Go 1.24+)
	u[0] = byte(milli >> 40)
	u[1] = byte(milli >> 32)
	u[2] = byte(milli >> 24)
	u[3] = byte(milli >> 16)
	u[4] = byte(milli >> 8)
	u[5] = byte(milli)
	u[6] = 0x70 | byte(seq>>8)&0x0f // version 7
	u[7] = byte(seq)
	u[8] = u[8]&0x3f | 0x80 // RFC 9562 variant

	var s [36]byte
	hex.Encode(s[0:8], u[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], u[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], u[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], u[8:10])
	s[23] = '-'
	hex.Encode(s[24:], u[10:])
	return string(s[:])
}

var v7 struct {
	sync.Mutex
	last int64 // milli<<12 + seq of the last id
}

// v7Clock returns the time part of the next id: Unix milliseconds and a
// 12-bit sequence from the sub-millisecond time, bumped so that ids never go
// backwards within the process (as google/uuid does).
func v7Clock() (milli, seq int64) {
	nano := time.Now().UnixNano()
	milli = nano / 1e6
	seq = (nano - milli*1e6) >> 8 // 0..3906
	v7.Lock()
	defer v7.Unlock()
	now := milli<<12 + seq
	if now <= v7.last {
		now = v7.last + 1
	}
	v7.last = now
	return now >> 12, now & 0xfff
}
