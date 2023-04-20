// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package roachtestutil

import (
	"context"
	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
)

// CheckReplicaDivergenceOnDB runs a stats-only consistency check via the
// provided DB. It ignores transient errors that can result from the
// implementation of crdb_internal.check_consistency, so a nil result
// does not prove anything.
func CheckReplicaDivergenceOnDB(ctx context.Context, l *logger.Logger, db *gosql.DB) error {
	// NB: we set a statement_timeout since context cancellation won't work here,
	// see:
	// https://github.com/cockroachdb/cockroach/pull/34520
	//
	// We've seen the consistency checks hang indefinitely in some cases.
	rows, err := db.QueryContext(ctx, `
SET statement_timeout = '5m';
SELECT t.range_id, t.start_key_pretty, t.status, t.detail
FROM
crdb_internal.check_consistency(true, '', '') as t
WHERE t.status NOT IN ('RANGE_CONSISTENT', 'RANGE_INDETERMINATE')`)
	if err != nil {
		// TODO(tbg): the checks can fail for silly reasons like missing gossiped
		// descriptors, etc. -- not worth failing the test for. Ideally this would
		// be rock solid.
		l.Printf("consistency check failed with %v; ignoring", err)
		return nil
	}
	defer rows.Close()
	var finalErr error
	for rows.Next() {
		var rangeID int32
		var prettyKey, status, detail string
		if scanErr := rows.Scan(&rangeID, &prettyKey, &status, &detail); scanErr != nil {
			l.Printf("consistency check failed with %v; ignoring", scanErr)
			return nil
		}
		finalErr = errors.CombineErrors(finalErr,
			errors.Newf("r%d (%s) is inconsistent: %s %s\n", rangeID, prettyKey, status, detail))
	}
	if err := rows.Err(); err != nil {
		l.Printf("consistency check failed with %v; ignoring", err)
		return nil
	}
	return finalErr
}

// CheckInvalidDescriptors returns an error if there exists any descriptors in
// the crdb_internal.invalid_objects virtual table.
func CheckInvalidDescriptors(ctx context.Context, db *gosql.DB) error {
	// Because crdb_internal.invalid_objects is a virtual table, by default, the
	// query will take a lease on the database sqlDB is connected to and only run
	// the query on the given database. The "" prefix prevents this lease
	// acquisition and allows the query to fetch all descriptors in the cluster.
	rows, err := db.QueryContext(ctx, `
SET statement_timeout = '1m'; 
SELECT id, obj_name, error FROM "".crdb_internal.invalid_objects`)
	if err != nil {
		return err
	}
	invalidIDs, err := sqlutils.RowsToDataDrivenOutput(rows)
	if err != nil {
		return err
	}
	if invalidIDs != "" {
		return errors.Errorf("the following descriptor ids are invalid\n%v", invalidIDs)
	}
	return nil
}
