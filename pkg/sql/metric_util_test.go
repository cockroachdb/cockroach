// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Functions used for testing metrics.

package sql_test

import (
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/pkg/errors"
)

// initializeQueryCounter returns a queryCounter that accounts for system
// migrations that may have run DDL statements.
func initializeQueryCounter(s serverutils.TestServerInterface) queryCounter {
	return queryCounter{
		txnBeginCount:                   s.MustGetSQLCounter(sql.MetaTxnBeginStarted.Name),
		selectCount:                     s.MustGetSQLCounter(sql.MetaSelectStarted.Name),
		optCount:                        s.MustGetSQLCounter(sql.MetaSQLOpt.Name),
		distSQLSelectCount:              s.MustGetSQLCounter(sql.MetaDistSQLSelect.Name),
		updateCount:                     s.MustGetSQLCounter(sql.MetaUpdateStarted.Name),
		insertCount:                     s.MustGetSQLCounter(sql.MetaInsertStarted.Name),
		deleteCount:                     s.MustGetSQLCounter(sql.MetaDeleteStarted.Name),
		ddlCount:                        s.MustGetSQLCounter(sql.MetaDdlStarted.Name),
		miscCount:                       s.MustGetSQLCounter(sql.MetaMiscStarted.Name),
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
