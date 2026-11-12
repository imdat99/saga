package saga

import "github.com/imdat99/saga/internal/uuidv7"

// newRunID returns a UUIDv7 string (see internal/uuidv7).
func newRunID() string { return uuidv7.New() }
