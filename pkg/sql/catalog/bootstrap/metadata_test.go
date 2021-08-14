// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bootstrap_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestArbitrarySystemDescriptorIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Arbitrarily offset system descriptors
	defer bootstrap.TestingSetDescriptorIDOffset(50)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(db)

	// Check that offset is property taken into account in system descriptors.
	const q1 = `
SELECT
	name, id
FROM
	system.namespace
WHERE
	"parentID" = 1 AND "parentSchemaID" = 29 AND name IN ('lease', 'jobs')
ORDER BY
	name ASC;`
	tdb.CheckQueryResults(t, q1, [][]string{{"jobs", "15"}, {"lease", "11"}})

	// Check that offset is property taken into account in descriptor creation.
	tdb.Exec(t, `CREATE DATABASE test`)
	const q2 = `
SELECT
	name, id
FROM
	system.namespace
WHERE
	"parentID" = 0 AND "parentSchemaID" = 0 AND name = 'test'
ORDER BY
	name ASC;`
	tdb.CheckQueryResults(t, q2, [][]string{{"test", "52"}})
}
