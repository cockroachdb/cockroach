// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCreateNoopSchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)

	// TODO(yevgeniy): Write real tests; for now just try to execute various
	// forms of create schedule for backup to test parsing.
	sqlDB.Exec(t, `CREATE SCHEDULE 'test' FOR BACKUP TO 'foo'`)

	sqlDB.Exec(t, ` 
CREATE SCHEDULE FOR BACKUP TO 'foo'
WITH key=value 
APPENDING CHANGES EVERY INTERVAL '1 hour'
`)

	sqlDB.Exec(t, ` 
CREATE SCHEDULE FOR BACKUP OF DATABASE a, b, c TO 'foo'
WITH key=value
ON PREVIOUS RUNNING SKIP
RECURRING '@daily' 
APPENDING CHANGES EVERY INTERVAL '1 hour'
`)
}
