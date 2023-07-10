// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestExecSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	server.SQLAPIClock = timeutil.NewManualTime(timeutil.FromUnixMicros(0))
	defer func() {
		server.SQLAPIClock = timeutil.DefaultTimeSource{}
	}()

	ctx := context.Background()

	testHelper := NewTestTenantHelper(
		t,
		3, /* tenantClusterSize */
		1, /* numNodes */
		base.TestingKnobs{},
	)
	defer testHelper.Cleanup(ctx, t)

	tenantCluster := testHelper.TestCluster()
	adminClient := tenantCluster.TenantAdminHTTPClient(t, 0)
	nonAdminClient := tenantCluster.TenantHTTPClient(t, 0, false)

	datadriven.RunTest(t, "testdata/api_v2_sql",
		func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd != "sql" {
				t.Fatal("Only sql command is accepted in this test")
			}

			var client *httpClient
			if d.HasArg("admin") {
				client = adminClient
			}
			if d.HasArg("non-admin") {
				client = nonAdminClient
			}

			resp, err := client.PostJSONRawChecked(
				"/api/v2/sql/",
				[]byte(d.Input),
			)
			require.NoError(t, err)
			defer resp.Body.Close()

			r, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if d.HasArg("expect-error") {
				type jsonError struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				}
				type errorResp struct {
					Error jsonError `json:"error"`
				}

				er := errorResp{}
				err := json.Unmarshal(r, &er)
				require.NoError(t, err)
				return fmt.Sprintf("%s|%s", er.Error.Code, er.Error.Message)
			}
			var u interface{}
			err = json.Unmarshal(r, &u)
			require.NoError(t, err)
			s, err := json.MarshalIndent(u, "", " ")
			require.NoError(t, err)
			return string(s)
		},
	)
}
