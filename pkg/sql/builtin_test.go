// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestFuncNull execs all builtin funcs with various kinds of NULLs,
// attempting to induce a panic.
func TestFuncNull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	run := func(t *testing.T, q string) {
		rows, err := db.QueryContext(ctx, q)
		if err == nil {
			rows.Close()
		}
	}

	for _, name := range builtins.AllBuiltinNames() {
		switch strings.ToLower(name) {
		case "crdb_internal.force_panic", "crdb_internal.force_log_fatal", "pg_sleep":
			continue
		}
		_, variations := builtinsregistry.GetBuiltinProperties(name)
		for _, builtin := range variations {
			// Untyped NULL.
			{
				var sb strings.Builder
				fmt.Fprintf(&sb, "SELECT %s(", name)
				for i := range builtin.Types.Types() {
					if i > 0 {
						sb.WriteString(", ")
					}
					sb.WriteString("NULL")
				}
				sb.WriteString(")")
				run(t, sb.String())
			}
			// Typed NULL.
			{
				var sb strings.Builder
				fmt.Fprintf(&sb, "SELECT %s(", name)
				for i, typ := range builtin.Types.Types() {
					if i > 0 {
						sb.WriteString(", ")
					}
					fmt.Fprintf(&sb, "NULL::%s", typ)
				}
				sb.WriteString(")")
				run(t, sb.String())
			}
			// NULL that the type system can't (at least not yet?) know is NULL.
			{
				var sb strings.Builder
				fmt.Fprintf(&sb, "SELECT %s(", name)
				for i, typ := range builtin.Types.Types() {
					if i > 0 {
						sb.WriteString(", ")
					}
					fmt.Fprintf(&sb, "(SELECT NULL)::%s", typ)
				}
				sb.WriteString(")")
				run(t, sb.String())
			}
			// For array types, make an array with a NULL.
			{
				var sb strings.Builder
				fmt.Fprintf(&sb, "SELECT %s(", name)
				hasArray := false
				for i, typ := range builtin.Types.Types() {
					if i > 0 {
						sb.WriteString(", ")
					}
					if typ.Family() == types.ArrayFamily {
						hasArray = true
						if typ.ArrayContents().Family() == types.AnyFamily {
							fmt.Fprintf(&sb, "ARRAY[NULL]::STRING[]")
						} else {
							fmt.Fprintf(&sb, "ARRAY[NULL]::%s", typ)
						}
					} else {
						fmt.Fprintf(&sb, "NULL::%s", typ)
					}
				}
				if hasArray {
					sb.WriteString(")")
					run(t, sb.String())
				}
			}
		}
	}
}

// TestRoleNameFromOID validates that builtin roles can correctly
// have their OID resolved inside: RoleNameFromOID.
func TestRoleNameFromOID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := &planner{}
	ctx := context.Background()

	// Use the same OID hashing logic as the server.
	h := makeOidHasher()

	testCases := []struct {
		name     username.SQLUsername
		expected string
	}{
		{username.NodeUserName(), username.NodeUser},
		{username.RootUserName(), username.RootUser},
		{username.AdminRoleName(), username.AdminRole},
		{username.PublicRoleName(), username.PublicRole},
	}

	for _, tc := range testCases {
		oidVal := h.UserOid(tc.name).Oid
		name, ok := p.MaybeResolveSystemRoleOID(ctx, oidVal)
		if !ok {
			t.Errorf("expected fast-path match for %s (OID %d)", tc.name, oidVal)
			continue
		}
		if name != tc.expected {
			t.Errorf("expected %s, got %s", tc.expected, name)
		}
	}

	// Test unknown OID
	if _, ok := p.MaybeResolveSystemRoleOID(ctx, 123456); ok {
		t.Error("expected no match for unknown OID")
	}
}

// TestSystemRoleOIDResolution validates that pg_has_role does not
// read to read system tables to resolve builtin roles.
func TestSystemRoleOIDResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Verify that pg_has_role (which uses getNameForArg) works correctly for
	// system roles. These roles have OIDs that are deterministic hashes of
	// their names.
	systemRoles := []string{"node", "root", "admin"}

	for _, role := range systemRoles {
		t.Run(role, func(t *testing.T) {
			var oid int64
			query := fmt.Sprintf("SELECT oid FROM pg_catalog.pg_roles WHERE rolname = '%s'", role)
			err := db.QueryRowContext(ctx, query).Scan(&oid)
			if err != nil {
				t.Fatalf("failed to get OID for %s: %v", role, err)
			}

			// Test pg_has_role. This triggers the getNameForArg code path.
			// Explicitly cast to OID to ensure we hit the DOid case in getNameForArg.
			var hasRole bool
			err = db.QueryRowContext(ctx, "SELECT pg_catalog.pg_has_role($1::OID, 'USAGE')", oid).Scan(&hasRole)
			if err != nil {
				t.Errorf("pg_has_role failed for %s (OID %d): %v", role, oid, err)
			}
		})
	}
}
