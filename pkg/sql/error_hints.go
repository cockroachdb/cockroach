// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/errors"
)

// addPlanningErrorHints is responsible for enhancing the provided
// error object with hints that inform the user about their options.
func addPlanningErrorHints(
	ctx context.Context, err error, stmt *Statement, st *cluster.Settings, ns eval.ClientNoticeSender,
) error {
	pgCode := pgerror.GetPGCode(err)
	switch pgCode {
	case pgcode.UndefinedColumn:
		resolvedSQL := stmt.AST.String()

		// Changes introduced in v23.1.
		extraRangeDoc := false
		switch {
		case strings.Contains(resolvedSQL, "SHOW RANGES"):
			errS := err.Error()

			// The following columns are not available when using SHOW
			// RANGES after the introduction of coalesced ranges.
			if strings.Contains(errS, "lease_holder") || strings.Contains(errS, "range_size") {
				err = errors.WithHint(err,
					"To list lease holder and range size details, consider SHOW RANGES WITH DETAILS.")
				extraRangeDoc = true
			}

		case strings.Contains(resolvedSQL, "crdb_internal.ranges" /* also matches ranges_no_leases */):
			errS := err.Error()

			// The following columns are not available when using SHOW
			// RANGES after the introduction of coalesced ranges.
			if strings.Contains(errS, "database_name") {
				err = errors.WithHint(err,
					"To list all ranges across all databases and display the database name, consider SHOW CLUSTER RANGES WITH TABLES.")
				extraRangeDoc = true
			}
			if strings.Contains(errS, "schema_name") ||
				strings.Contains(errS, "table_name") ||
				strings.Contains(errS, "table_id") {
				err = errors.WithHint(err,
					"To retrieve table/schema names/IDs associated with ranges, consider SHOW [CLUSTER] RANGES WITH TABLES.")
				extraRangeDoc = true
			}
			if strings.Contains(errS, "index_name") {
				err = errors.WithHint(err,
					"To retrieve index names associated with ranges, consider SHOW [CLUSTER] RANGES WITH INDEXES.")
				extraRangeDoc = true
			}
		}
		if extraRangeDoc {
			err = errors.WithHint(err,
				"There are more SHOW RANGES options. Refer to the online documentation or execute 'SHOW RANGES ??' for details.")
		}
	}
	return err
}
