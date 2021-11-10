// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestPublicSchemaMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors - 1),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	defer db.Close()
	//kvDB := tc.Server(0).DB()

	db.Exec(`RESTORE FROM '2021/05/21-020411.00' IN
				'gs://cockroach-fixtures/tpcc-incrementals?AUTH=implicit'
				AS OF SYSTEM TIME '2021-05-21 14:40:22'`)
	_, err := db.Exec(`CREATE TABLE defaultdb.public.t(x INT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO defaultdb.public.t VALUES (1), (2), (3)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TYPE defaultdb.public.typ AS ENUM()`)
	require.NoError(t, err)

	var x int
	rows1, err := db.Query(`SELECT * FROM defaultdb.public.t ORDER BY x`)
	defer rows1.Close()
	require.NoError(t, err)
	for rows1.Next() {
		err = rows1.Scan(&x)
		require.NoError(t, err)
		fmt.Println("x:", x)
	}

	var parentID, parentSchemaID, id int
	var name string
	rows, err := db.Query(`SELECT * FROM system.namespace ORDER BY ID`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&parentID, &parentSchemaID, &name, &id)
		require.NoError(t, err)
		fmt.Println(parentID, parentSchemaID, name, id)
	}

	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors).String())
	require.NoError(t, err)

	rows, err = db.Query(`SELECT * FROM system.namespace ORDER BY ID`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&parentID, &parentSchemaID, &name, &id)
		require.NoError(t, err)
		fmt.Println(parentID, parentSchemaID, name, id)
	}

	rows, err = db.Query(`SELECT * FROM defaultdb.public.t ORDER BY x`)
	require.NoError(t, err)
	defer rows.Close()
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		require.NoError(t, err)
		err = rows.Scan(&x)
		require.NoError(t, err)
		fmt.Println("x2", x)
	}
}
