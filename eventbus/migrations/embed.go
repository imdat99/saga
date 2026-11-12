// Package migrations embeds the eventbus schema migrations.
package migrations

import "embed"

// FS holds the goose SQL migrations of the eventbus schema.
//
//go:embed *.sql
var FS embed.FS
