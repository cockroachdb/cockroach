// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFixCastForStyle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.DateStyleIntervalStyleCastRewrite - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	sqlDB := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	tdb.Exec(t, `CREATE TABLE ds (
it interval,
s string,
t timestamp,
c string  AS ((it + interval '2 minutes')::string) STORED,
c2 interval AS ((s)::interval) STORED,
c3 string AS (t::string) STORED,
c4 interval AS ((s)::INTERVAL(4)) STORED
)`)
	tdb.Exec(t, `CREATE INDEX rw ON ds ((it::text))`)
	tdb.Exec(t, `CREATE INDEX partial ON ds(it) WHERE (it::text) > 'abc';`)

	tdb.Exec(t, `CREATE TABLE ds2 (
	ch char,
	d date,
	c string AS (d::string) STORED,
	c1 string AS (lower(d::STRING)) STORED,
	c2 interval AS (ch::interval) STORED
)`)

	upgrades.Migrate(
		t,
		sqlDB,
		clusterversion.DateStyleIntervalStyleCastRewrite,
		nil,
		false,
	)

	tests := []struct {
		tName  string
		expect string
	}{
		{
			tName: "ds",
			expect: `CREATE TABLE public.ds (
	it INTERVAL NULL,
	s STRING NULL,
	t TIMESTAMP NULL,
	c STRING NULL AS (to_char(it + '00:02:00':::INTERVAL)) STORED,
	c2 INTERVAL NULL AS (parse_interval(s)) STORED,
	c3 STRING NULL AS (to_char(t)) STORED,
	c4 INTERVAL NULL AS (parse_interval(s)::INTERVAL(4)) STORED,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT ds_pkey PRIMARY KEY (rowid ASC),
	INDEX rw (to_char(it) ASC),
	INDEX partial (it ASC) WHERE to_char(it) > 'abc':::STRING
)`,
		},
		{
			tName: "ds2",
			expect: `CREATE TABLE public.ds2 (
	ch CHAR NULL,
	d DATE NULL,
	c STRING NULL AS (to_char(d)) STORED,
	c1 STRING NULL AS (lower(to_char(d))) STORED,
	c2 INTERVAL NULL AS (parse_interval(ch)) STORED,
	rowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),
	CONSTRAINT ds2_pkey PRIMARY KEY (rowid ASC)
)`,
		},
	}

	for _, test := range tests {
		t.Run(test.tName, func(t *testing.T) {
			row := sqlDB.QueryRow(fmt.Sprintf("SHOW CREATE %s", test.tName))
			var other, create string
			err := row.Scan(&other, &create)
			require.NoError(t, err)
			require.Equal(t, test.expect, create)
		})
	}
}
