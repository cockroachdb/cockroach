// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
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

	for _, name := range builtins.AllBuiltinNames {
		switch strings.ToLower(name) {
		case "crdb_internal.force_panic", "crdb_internal.force_log_fatal", "pg_sleep":
			continue
		}
		_, variations := builtins.GetBuiltinProperties(name)
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
