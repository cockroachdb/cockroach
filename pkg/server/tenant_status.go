// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// tenantStatusServer is an implementation of a SQLStatusServer that is
// available to tenants. The full statusServer implementation is unavailable to
// tenants due to its use of gossip and other unavailable subsystems.
// The tenantStatusServer implementation is local only. This is enough for
// Phase 2 requirements that there can only be at most one live SQL pod per
// tenant.
type tenantStatusServer struct {
	baseStatusServer
}

func newTenantStatusServer(
	ambient log.AmbientContext,
	privilegeChecker *adminPrivilegeChecker,
	sessionRegistry *sql.SessionRegistry,
	st *cluster.Settings,
) *tenantStatusServer {
	ambient.AddLogTag("tenant-status", nil)
	return &tenantStatusServer{
		baseStatusServer: baseStatusServer{
			AmbientContext:   ambient,
			privilegeChecker: privilegeChecker,
			sessionRegistry:  sessionRegistry,
			st:               st,
		},
	}
}

func (t *tenantStatusServer) ListSessions(
	ctx context.Context, request *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	return t.ListLocalSessions(ctx, request)
}

func (t *tenantStatusServer) ListLocalSessions(
	ctx context.Context, request *serverpb.ListSessionsRequest,
) (*serverpb.ListSessionsResponse, error) {
	sessions, err := t.getLocalSessions(ctx, request)
	if err != nil {
		return nil, err
	}
	return &serverpb.ListSessionsResponse{Sessions: sessions}, nil
}

func (t *tenantStatusServer) CancelQuery(
	ctx context.Context, request *serverpb.CancelQueryRequest,
) (*serverpb.CancelQueryResponse, error) {
	reqUsername := security.MakeSQLUsernameFromPreNormalizedString(request.Username)
	if err := t.checkCancelPrivilege(ctx, reqUsername, findSessionByQueryID(request.QueryID)); err != nil {
		return nil, err
	}
	var (
		output = &serverpb.CancelQueryResponse{}
		err    error
	)
	output.Canceled, err = t.sessionRegistry.CancelQuery(request.QueryID)
	if err != nil {
		output.Error = err.Error()
	}
	return output, nil
}

func (t *tenantStatusServer) CancelSession(
	ctx context.Context, request *serverpb.CancelSessionRequest,
) (*serverpb.CancelSessionResponse, error) {
	reqUsername := security.MakeSQLUsernameFromPreNormalizedString(request.Username)
	if err := t.checkCancelPrivilege(ctx, reqUsername, findSessionBySessionID(request.SessionID)); err != nil {
		return nil, err
	}
	return t.sessionRegistry.CancelSession(request.SessionID)
}
