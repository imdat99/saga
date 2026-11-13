package eventbus

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestUUIDTextRoundTrip(t *testing.T) {
	for range 100 {
		u := uuid.New()
		id := [16]byte(u)
		if got := string(appendUUID(nil, &id)); got != u.String() {
			t.Fatalf("appendUUID = %s, want %s", got, u)
		}
		var back [16]byte
		if err := parseUUID(&back, []byte(u.String()), pgx.TextFormatCode); err != nil || back != id {
			t.Fatalf("parse text %s: %x, %v", u, back, err)
		}
		if err := parseUUID(&back, id[:], pgx.BinaryFormatCode); err != nil || back != id {
			t.Fatalf("parse binary: %v", err)
		}
	}
	var x [16]byte
	for _, bad := range []string{"", "0196a0b2-0000-7000-8000-00000000000", "0196a0b2x0000-7000-8000-000000000001", "zz96a0b2-0000-7000-8000-000000000001"} {
		if err := parseUUID(&x, []byte(bad), pgx.TextFormatCode); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if err := parseUUID(&x, []byte{1, 2}, pgx.BinaryFormatCode); err == nil {
		t.Error("accepted 2 binary bytes")
	}
}
