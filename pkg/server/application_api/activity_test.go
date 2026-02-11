// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestListActivitySecurity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	expectedErrNoPermission := "this operation requires the VIEWACTIVITY or VIEWACTIVITYREDACTED system privilege"
	contentionMsg := &serverpb.ListContentionEventsResponse{}
	flowsMsg := &serverpb.ListDistSQLFlowsResponse{}
	getErrors := func(msg protoutil.Message) []serverpb.ListActivityError {
		switch r := msg.(type) {
		case *serverpb.ListContentionEventsResponse:
			return r.Errors
		case *serverpb.ListDistSQLFlowsResponse:
			return r.Errors
		default:
			t.Fatal("unexpected message type")
			return nil
		}
	}

	// HTTP requests respect the authenticated username from the HTTP session.
	testCases := []struct {
		endpoint                       string
		expectedErr                    string
		requestWithAdmin               bool
		requestWithViewActivityGranted bool
		response                       protoutil.Message
	}{
		{"local_contention_events", expectedErrNoPermission, false, false, contentionMsg},
		{"contention_events", expectedErrNoPermission, false, false, contentionMsg},
		{"local_contention_events", "", true, false, contentionMsg},
		{"contention_events", "", true, false, contentionMsg},
		{"local_contention_events", "", false, true, contentionMsg},
		{"contention_events", "", false, true, contentionMsg},
		{"local_distsql_flows", expectedErrNoPermission, false, false, flowsMsg},
		{"distsql_flows", expectedErrNoPermission, false, false, flowsMsg},
		{"local_distsql_flows", "", true, false, flowsMsg},
		{"distsql_flows", "", true, false, flowsMsg},
		{"local_distsql_flows", "", false, true, flowsMsg},
		{"distsql_flows", "", false, true, flowsMsg},
	}
	myUser := apiconstants.TestingUserNameNoAdmin().Normalized()
	for _, tc := range testCases {
		if tc.requestWithViewActivityGranted {
			// Note that for this query to work, it is crucial that
			// srvtestutils.GetStatusJSONProtoWithAdminOption below is called at least once,
			// on the previous test case, so that the user exists.
			_, err := db.Exec(fmt.Sprintf("ALTER USER %s VIEWACTIVITY", myUser))
			require.NoError(t, err)
		}
		err := srvtestutils.GetStatusJSONProtoWithAdminOption(s, tc.endpoint, tc.response, tc.requestWithAdmin)
		responseErrors := getErrors(tc.response)
		if tc.expectedErr == "" {
			if err != nil || len(responseErrors) > 0 {
				t.Errorf("unexpected failure listing the activity; error: %v; response errors: %v",
					err, responseErrors)
			}
		} else {
			respErr := "<no error>"
			if len(responseErrors) > 0 {
				respErr = responseErrors[0].Message
			}
			if !testutils.IsError(err, tc.expectedErr) &&
				!strings.Contains(respErr, tc.expectedErr) {
				t.Errorf("did not get expected error %q when listing the activity from %s: %v",
					tc.expectedErr, tc.endpoint, err)
			}
		}
		if tc.requestWithViewActivityGranted {
			_, err := db.Exec(fmt.Sprintf("ALTER USER %s NOVIEWACTIVITY", myUser))
			require.NoError(t, err)
		}
	}

	// gRPC requests behave as root and thus are always allowed.
	client := s.GetStatusClient(t)
	{
		request := &serverpb.ListContentionEventsRequest{}
		if resp, err := client.ListLocalContentionEvents(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing local contention events; error: %v; response errors: %v",
				err, resp.Errors)
		}
		if resp, err := client.ListContentionEvents(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing contention events; error: %v; response errors: %v",
				err, resp.Errors)
		}
	}
	{
		request := &serverpb.ListDistSQLFlowsRequest{}
		if resp, err := client.ListLocalDistSQLFlows(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing local distsql flows; error: %v; response errors: %v",
				err, resp.Errors)
		}
		if resp, err := client.ListDistSQLFlows(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing distsql flows; error: %v; response errors: %v",
				err, resp.Errors)
		}
	}
}
