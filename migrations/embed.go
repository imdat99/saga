// Package migrations embeds the SQL migrations of the saga schema.
package migrations

import "embed"

// FS holds the goose migrations for the saga schema.
//
//go:embed *.sql
var FS embed.FS
