// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Functions used for testing metrics.

package sql_test

import (
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// initializeQueryCounter returns a queryCounter that accounts for system
// migrations that may have run DDL statements.
func initializeQueryCounter(s serverutils.TestServerInterface) queryCounter {
	return queryCounter{
		txnBeginCount:                   s.MustGetSQLCounter(sql.MetaTxnBeginStarted.Name),
		selectCount:                     s.MustGetSQLCounter(sql.MetaSelectStarted.Name),
		selectExecutedCount:             s.MustGetSQLCounter(sql.MetaSelectExecuted.Name),
		distSQLSelectCount:              s.MustGetSQLCounter(sql.MetaDistSQLSelect.Name),
		updateCount:                     s.MustGetSQLCounter(sql.MetaUpdateStarted.Name),
		insertCount:                     s.MustGetSQLCounter(sql.MetaInsertStarted.Name),
		deleteCount:                     s.MustGetSQLCounter(sql.MetaDeleteStarted.Name),
		ddlCount:                        s.MustGetSQLCounter(sql.MetaDdlStarted.Name),
		miscCount:                       s.MustGetSQLCounter(sql.MetaMiscStarted.Name),
		miscExecutedCount:               s.MustGetSQLCounter(sql.MetaMiscExecuted.Name),
		txnCommitCount:                  s.MustGetSQLCounter(sql.MetaTxnCommitStarted.Name),
		txnRollbackCount:                s.MustGetSQLCounter(sql.MetaTxnRollbackStarted.Name),
		txnAbortCount:                   s.MustGetSQLCounter(sql.MetaTxnAbort.Name),
		savepointCount:                  s.MustGetSQLCounter(sql.MetaSavepointStarted.Name),
		restartSavepointCount:           s.MustGetSQLCounter(sql.MetaRestartSavepointStarted.Name),
		releaseRestartSavepointCount:    s.MustGetSQLCounter(sql.MetaReleaseRestartSavepointStarted.Name),
		rollbackToRestartSavepointCount: s.MustGetSQLCounter(sql.MetaRollbackToRestartSavepointStarted.Name),
	}
}

func checkCounterDelta(
	s serverutils.TestServerInterface, meta metric.Metadata, init, delta int64,
) (int64, error) {
	actual := s.MustGetSQLCounter(meta.Name)
	if actual != (init + delta) {
		return actual, errors.Errorf("query %s: actual %d != (init %d + delta %d)",
			meta.Name, actual, init, delta)
	}
	return actual, nil
}

func checkCounterGE(s serverutils.TestServerInterface, meta metric.Metadata, e int64) error {
	if a := s.MustGetSQLCounter(meta.Name); a < e {
		return errors.Errorf("stat %s: expected: actual %d >= %d",
			meta.Name, a, e)
	}
	return nil
}
