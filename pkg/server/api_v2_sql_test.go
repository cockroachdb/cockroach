// Copyright 2022 The Cockroach Authors.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestExecSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	SQLAPIClock = timeutil.NewManualTime(timeutil.FromUnixMicros(0))
	defer func() {
		SQLAPIClock = timeutil.DefaultTimeSource{}
	}()

	server, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer server.Stopper().Stop(ctx)

	adminClient, err := server.GetAdminHTTPClient()
	require.NoError(t, err)

	nonAdminClient, err := server.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	require.NoError(t, err)

	datadriven.RunTest(t, "testdata/api_v2_sql",
		func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd != "sql" {
				t.Fatal("Only sql command is accepted in this test")
			}

			var client http.Client
			if d.HasArg("admin") {
				client = adminClient
			}
			if d.HasArg("non-admin") {
				client = nonAdminClient
			}

			resp, err := client.Post(
				server.AdminURL()+"/api/v2/sql/", "application/json",
				bytes.NewReader([]byte(d.Input)),
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
