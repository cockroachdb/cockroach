// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func preconditionBeforeStartingAnUpgrade(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// precondition: make sure no invalid descriptors exist before starting an upgrade
	err := preconditionNoInvalidDescriptorsBeforeUpgrading(ctx, cs, d)
	if err != nil {
		return err
	}

	// Add other preconditions before starting an upgrade here.

	return nil
}

// preconditionNoInvalidDescriptorsBeforeUpgrading is a function
// that returns a non-nill error if there are any invalid descriptors.
// It is done by querying `crdb_internal.invalid_objects` and see if
// it is empty.
func preconditionNoInvalidDescriptorsBeforeUpgrading(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	var errMsg strings.Builder
	err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		query := `SELECT * FROM crdb_internal.invalid_objects`
		rows, err := txn.QueryIterator(
			ctx, "check-if-there-are-any-invalid-descriptors", txn.KV(), query)
		if err != nil {
			return err
		}
		var hasNext bool
		for hasNext, err = rows.Next(ctx); hasNext && err == nil; hasNext, err = rows.Next(ctx) {
			// There exists invalid objects; Accumulate their information into `errMsg`.
			// `crdb_internal.invalid_objects` has five columns: id, database name, schema name, table name, error.
			row := rows.Cur()
			descName := tree.MakeTableNameWithSchema(
				tree.Name(tree.MustBeDString(row[1])),
				tree.Name(tree.MustBeDString(row[2])),
				tree.Name(tree.MustBeDString(row[3])),
			)
			tableID := descpb.ID(tree.MustBeDInt(row[0]))
			errString := string(tree.MustBeDString(row[4]))
			// TODO(ssd): Remove in 23.1 once we are sure that the migration which fixes this corruption has run.
			if veryLikelyKnownUserfileBreakage(ctx, txn.KV(), txn.Descriptors(), tableID, errString) {
				log.Infof(ctx, "ignoring invalid descriptor %v (%v) with error %q because it looks like known userfile-related corruption",
					descName.String(), tableID, errString)
			} else {
				errMsg.WriteString(fmt.Sprintf("invalid descriptor: %v (%v) because %v\n", descName.String(), row[0], row[4]))
			}
		}
		return err
	})
	if err != nil {
		return err
	}
	if errMsg.Len() == 0 {
		return nil
	}
	return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
		"there exists invalid descriptors as listed below; fix these descriptors "+
			"before attempting to upgrade again:\n%v", errMsg.String())
}
