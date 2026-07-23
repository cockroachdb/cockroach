// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/ui"
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

	server := serverutils.StartServerOnly(t, base.TestServerArgs{})
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
			if d.HasArg("disable_database_locality_metadata") {
				ui.DatabaseLocalityMetadataEnabled.Override(ctx, &server.ClusterSettings().SV, false)
			}

			resp, err := client.Post(
				server.AdminURL().WithPath("/api/v2/sql/").String(), "application/json",
				bytes.NewReader([]byte(d.Input)),
			)
			require.NoError(t, err)
			defer resp.Body.Close()

			r, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if d.HasArg("expect-error") {
				code, msg := getErrorResponse(t, r)
				return fmt.Sprintf("%s|%s", code, msg)
			} else if d.HasArg("expect-no-error") {
				code, msg := getErrorResponse(t, r)
				require.True(t, code == "" && msg == "", code, msg)
				return ""
			}
			return getMarshalledResponse(t, r)
		},
	)
}

func getMarshalledResponse(t *testing.T, r []byte) string {
	type marshallJsonError struct {
		Code     string `json:"code"`
		Message  string `json:"message"`
		Severity string `json:"severity"`
	}
	// Type for the result.
	type marshalledTxnResult struct {
		Columns      interface{}        `json:"columns,omitempty"`
		End          interface{}        `json:"end"` // end timestamp.
		Error        *marshallJsonError `json:"error,omitempty"`
		Rows         interface{}        `json:"rows,omitempty"`
		RowsAffected int                `json:"rows_affected"`
		Start        interface{}        `json:"start"`     // start timestamp.
		Statement    int                `json:"statement"` // index of statement in request.
		Tag          string             `json:"tag"`       // SQL statement tag.
	}
	type marshalledExecResult struct {
		Retries    int                   `json:"retries,omitempty"`
		TxnResults []marshalledTxnResult `json:"txn_results"`
	}

	type marshalledResponse struct {
		Error         *marshallJsonError    `json:"error,omitempty"`
		Execution     *marshalledExecResult `json:"execution,omitempty"`
		NumStatements int                   `json:"num_statements,omitempty"`
		Request       interface{}           `json:"request,omitempty"`
	}

	var u = &marshalledResponse{}
	err := json.Unmarshal(r, u)
	require.NoError(t, err)
	s, err := json.MarshalIndent(u, "", " ")
	require.NoError(t, err)
	return string(s)
}

func getErrorResponse(t *testing.T, r []byte) (code, message string) {
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
	return er.Error.Code, er.Error.Message
}

func TestLocalityMetadataEnabledFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name        string
		query       string
		expectMatch bool
	}{
		{
			"db query",
			"SELECT array_agg(DISTINCT unnested_store_ids) AS store_ids" +
				" FROM [SHOW RANGES FROM DATABASE %1], unnest(replicas) AS unnested_store_ids",
			true,
		},
		{
			"table query",
			"SELECT count(unnested) AS replica_count, array_agg(DISTINCT unnested) AS store_ids" +
				" FROM [SHOW RANGES FROM TABLE %1], unnest(replicas) AS unnested;",
			true,
		},
		{
			"simpler matching query",
			"SHOW RANGES FROM DATABASE abc",
			true,
		},
		{
			"non-matching query",
			"SHOW RANGE FROM INDEX abc",
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectMatch, localityMetadataQueryRegexp.Match([]byte(tc.query)))
		})
	}
}
