// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestUsersV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	conn := testCluster.ServerConn(0)
	_, err := conn.Exec("CREATE USER test;")
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	client, err := ts1.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"users/", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	var ur usersResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ur))
	require.NoError(t, resp.Body.Close())

	require.Equal(t, 3, len(ur.Users))
	require.Contains(t, ur.Users, serverpb.UsersResponse_User{Username: "root"})
	require.Contains(t, ur.Users, serverpb.UsersResponse_User{Username: "test"})
}

func TestDatabasesTablesV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	conn := testCluster.ServerConn(0)
	_, err := conn.Exec("CREATE DATABASE testdb;")
	require.NoError(t, err)
	_, err = conn.Exec("CREATE TABLE testdb.testtable (id INTEGER PRIMARY KEY, value STRING);")
	require.NoError(t, err)
	_, err = conn.Exec("CREATE USER testuser WITH PASSWORD testpassword;")
	require.NoError(t, err)
	_, err = conn.Exec("GRANT ALL ON DATABASE testdb TO testuser;")
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	client, err := ts1.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)
	defer client.CloseIdleConnections()

	req, err := http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"databases/", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	var dr databasesResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&dr))
	require.NoError(t, resp.Body.Close())

	require.Contains(t, dr.Databases, "testdb")

	req, err = http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"databases/testdb/", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	var ddr databaseDetailsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ddr))
	require.NoError(t, resp.Body.Close())

	req, err = http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"databases/testdb/grants/", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	var dgr databaseGrantsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&dgr))
	require.NoError(t, resp.Body.Close())
	require.NotEmpty(t, dgr.Grants)

	req, err = http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"databases/testdb/tables/", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	var dtr databaseTablesResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&dtr))
	require.NoError(t, resp.Body.Close())
	require.Contains(t, dtr.TableNames, "public.testtable")

	// Test that querying the wrong db name returns 404.
	req, err = http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"databases/testdb2/tables/", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 404, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	req, err = http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"databases/testdb/tables/public.testtable/", nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	var tdr tableDetailsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&tdr))
	require.NoError(t, resp.Body.Close())

	require.Contains(t, tdr.CreateTableStatement, "value")
	var columns []string
	for _, c := range tdr.Columns {
		columns = append(columns, c.Name)
	}
	require.Contains(t, columns, "id")
	require.Contains(t, columns, "value")
}

func TestEventsV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	conn := testCluster.ServerConn(0)
	_, err := conn.Exec("CREATE DATABASE testdb;")
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	client, err := ts1.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"events/", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	var er eventsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&er))
	require.NoError(t, resp.Body.Close())
	found := false
	for _, e := range er.Events {
		if e.EventType == "create_database" && strings.Contains(e.Info, "testdb") {
			found = true
		}
	}
	require.True(t, found, "create_database event not found")
}
