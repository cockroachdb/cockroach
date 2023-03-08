// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"google.golang.org/grpc"
)

// WrappedServerStream is exported for testing.
type WrappedServerStream = wrappedServerStream

// TestingNewWrappedServerStream constructs a WrappedServerStream for testing.
// This function returns the more generic interface to defeat the linter
// which complains about returning an unexported type. Callers can type
// assert the return value into the above-exported *WrappedServerStream.
func TestingNewWrappedServerStream(
	ctx context.Context, ss grpc.ServerStream, recvFunc func(interface{}) error,
) grpc.ServerStream {
	return &WrappedServerStream{
		ServerStream: ss,
		ctx:          ctx,
		recv:         recvFunc,
	}
}

// TestingAuthenticateTenant performs authentication of a tenant from a context
// for testing.
func TestingAuthenticateTenant(
	ctx context.Context, serverTenantID roachpb.TenantID,
) (roachpb.TenantID, error) {
	_, authz, err := kvAuth{tenant: tenantAuthorizer{tenantID: serverTenantID}}.authenticateAndSelectAuthzRule(ctx)
	if err != nil {
		return roachpb.TenantID{}, err
	}
	switch z := authz.(type) {
	case authzTenantServerToKVServer:
		return roachpb.TenantID(z), nil
	default:
		return roachpb.TenantID{}, nil
	}
}

// TestingAuthorizeTenantRequest performs authorization of a tenant request
// for testing.
func TestingAuthorizeTenantRequest(
	ctx context.Context,
	sv *settings.Values,
	tenID roachpb.TenantID,
	method string,
	request interface{},
	authorizer tenantcapabilities.Authorizer,
) error {
	return tenantAuthorizer{
		tenantID:               tenID,
		capabilitiesAuthorizer: authorizer,
	}.authorize(ctx, sv, tenID, method, request)
}
