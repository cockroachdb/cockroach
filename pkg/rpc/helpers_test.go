// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	ctx context.Context,
	serverTenantID roachpb.TenantID,
	clusterSettings map[settings.InternalKey]settings.EncodedValue,
) (roachpb.TenantID, error) {
	sv := &settings.Values{}
	sv.Init(ctx, settings.TestOpaque)
	u := settings.NewUpdater(sv)

	kvAuthObject := kvAuth{sv: sv, tenant: tenantAuthorizer{tenantID: serverTenantID}}
	for setting := range clusterSettings {
		err := u.Set(ctx, setting, clusterSettings[setting])
		if err != nil {
			return roachpb.TenantID{}, err
		}
	}
	_, authz, err := kvAuthObject.authenticateAndSelectAuthzRule(ctx)
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
