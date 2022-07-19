// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func preconditionBeforeStartingAnUpgrade(
	ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps,
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
	ctx context.Context, cs clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	query := `SELECT * FROM crdb_internal.invalid_objects`
	rows, err := d.InternalExecutor.QueryIterator(
		ctx, "check-if-there-are-any-invalid-descriptors", nil /* txn */, query,
	)
	if err != nil {
		return err
	}

	var hasNext bool
	var errMsg strings.Builder
	for hasNext, err = rows.Next(ctx); hasNext && err == nil; hasNext, err = rows.Next(ctx) {
		// There exists invalid objects; Accumulate their information into `errMsg`.
		// `crdb_internal.invalid_objects` has five columns: id, database name, schema name, table name, error.
		row := rows.Cur()
		descName := tree.MakeTableNameWithSchema(
			tree.Name(tree.MustBeDString(row[1])),
			tree.Name(tree.MustBeDString(row[2])),
			tree.Name(tree.MustBeDString(row[3])),
		)
		errMsg.WriteString(fmt.Sprintf("Invalid descriptor: %v (%v) because %v\n", descName.String(), row[0], row[4]))
	}
	if err != nil {
		return err
	}

	if errMsg.Len() == 0 {
		return nil
	}
	return errors.AssertionFailedf("There exists invalid descriptors as listed below. Fix these descriptors "+
		"before attempting to upgrade again.\n%v", errMsg.String())
}
