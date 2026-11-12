// Package pgrole holds the database helpers shared by the example modules.
// Modules may import it; they may not import each other (depguard).
package pgrole

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var roleRE = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// InRoleTx runs fn in a transaction executed as role (SET LOCAL ROLE): a
// module can only touch the tables its role is granted, even though all
// modules share one pool.
func InRoleTx(ctx context.Context, pool *pgxpool.Pool, role string, fn func(tx pgx.Tx) error) error {
	if !roleRE.MatchString(role) {
		return fmt.Errorf("invalid role %q", role)
	}
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+role); err != nil {
			return fmt.Errorf("set role %s: %w", role, err)
		}
		return fn(tx)
	})
}

// migrateLockID serializes module migrations across instances starting at
// once: CREATE ROLE / SCHEMA / TABLE ... IF NOT EXISTS still race on the
// catalog's unique indexes.
const migrateLockID = 0x5a6a_0003

// EnsureRole creates schema and a NOLOGIN role with access to it, and lets the
// current user switch to the role. ddl runs as the current user after the
// schema exists (tables). Everything runs in one transaction under an
// advisory lock. Roles are cluster-wide: modules of different databases on
// one server share them.
func EnsureRole(ctx context.Context, pool *pgxpool.Pool, schema, role string, ddl string) error {
	if !roleRE.MatchString(role) || !roleRE.MatchString(schema) {
		return fmt.Errorf("invalid schema %q or role %q", schema, role)
	}
	stmts := []string{
		// Roles are cluster-wide but advisory locks are per database: another
		// database on the same server may create the role concurrently.
		`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + role + `') THEN
				BEGIN
					CREATE ROLE ` + role + ` NOLOGIN;
				EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
				END;
			END IF;
		END $$`,
		`DO $$ BEGIN
			IF NOT pg_has_role(current_user, '` + role + `', 'MEMBER') THEN
				EXECUTE format('GRANT ` + role + ` TO %I', current_user);
			END IF;
		END $$`,
		`CREATE SCHEMA IF NOT EXISTS ` + schema,
		ddl,
		`GRANT USAGE ON SCHEMA ` + schema + ` TO ` + role,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ` + schema + ` TO ` + role,
	}
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrateLockID); err != nil {
			return fmt.Errorf("setup %s: lock: %w", schema, err)
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("setup %s: %w", schema, err)
			}
		}
		return nil
	})
}
