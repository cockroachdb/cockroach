// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package srvtestutils

import (
	"context"
	"encoding/json"
	"io"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// GetAdminJSONProto performs a RPC-over-HTTP request to the admin endpoint
// and unmarshals the response into the specified proto message.
func GetAdminJSONProto(
	ts serverutils.TestTenantInterface, path string, response protoutil.Message,
) error {
	return GetAdminJSONProtoWithAdminOption(ts, path, response, true)
}

// GetAdminJSONProtoWithAdminOption performs a RPC-over-HTTP request to
// the admin endpoint and unmarshals the response into the specified
// proto message. It allows the caller to control whether the request
// is made with the admin role.
func GetAdminJSONProtoWithAdminOption(
	ts serverutils.TestTenantInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, apiconstants.AdminPrefix+path, response, isAdmin)
}

// PostAdminJSONProto performs a RPC-over-HTTP request to the admin endpoint
// and unmarshals the response into the specified proto message.
func PostAdminJSONProto(
	ts serverutils.TestTenantInterface, path string, request, response protoutil.Message,
) error {
	return PostAdminJSONProtoWithAdminOption(ts, path, request, response, true)
}

// PostAdminJSONProtoWithAdminOption performs a RPC-over-HTTP request to
// the admin endpoint and unmarshals the response into the specified
// proto message. It allows the caller to control whether the request
// is made with the admin role.
func PostAdminJSONProtoWithAdminOption(
	ts serverutils.TestTenantInterface,
	path string,
	request, response protoutil.Message,
	isAdmin bool,
) error {
	return serverutils.PostJSONProtoWithAdminOption(ts, apiconstants.AdminPrefix+path, request, response, isAdmin)
}

// GetStatusJSONProto performs a RPC-over-HTTP request to the status endpoint
// and unmarshals the response into the specified proto message.
func GetStatusJSONProto(
	ts serverutils.TestTenantInterface, path string, response protoutil.Message,
) error {
	return serverutils.GetJSONProto(ts, apiconstants.StatusPrefix+path, response)
}

// PostStatusJSONProto performs a RPC-over-HTTP request to the status endpoint
// and unmarshals the response into the specified proto message.
func PostStatusJSONProto(
	ts serverutils.TestTenantInterface, path string, request, response protoutil.Message,
) error {
	return serverutils.PostJSONProto(ts, apiconstants.StatusPrefix+path, request, response)
}

// GetStatusJSONProtoWithAdminOption performs a RPC-over-HTTP request to
// the status endpoint and unmarshals the response into the specified
// proto message. It allows the caller to control whether the request
// is made with the admin role.
func GetStatusJSONProtoWithAdminOption(
	ts serverutils.TestTenantInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	return serverutils.GetJSONProtoWithAdminOption(ts, apiconstants.StatusPrefix+path, response, isAdmin)
}

// PostStatusJSONProtoWithAdminOption performs a RPC-over-HTTP request to
// the status endpoint and unmarshals the response into the specified
// proto message. It allows the caller to control whether the request
// is made with the admin role.
func PostStatusJSONProtoWithAdminOption(
	ts serverutils.TestTenantInterface,
	path string,
	request, response protoutil.Message,
	isAdmin bool,
) error {
	return serverutils.PostJSONProtoWithAdminOption(ts, apiconstants.StatusPrefix+path, request, response, isAdmin)
}

// GetText fetches the HTTP response body as text in the form of a
// byte slice from the specified URL.
func GetText(ts serverutils.TestTenantInterface, url string) ([]byte, error) {
	httpClient, err := ts.GetAdminHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// GetJSON fetches the JSON from the specified URL and returns
// it as unmarshaled JSON. Returns an error on any failure to fetch
// or unmarshal response body.
func GetJSON(ts serverutils.TestTenantInterface, url string) (interface{}, error) {
	body, err := GetText(ts, url)
	if err != nil {
		return nil, err
	}
	var jI interface{}
	if err := json.Unmarshal(body, &jI); err != nil {
		return nil, errors.Wrapf(err, "body is:\n%s", body)
	}
	return jI, nil
}

// NewRPCTestContext constructs a RPC context for use in API tests.
func NewRPCTestContext(
	ctx context.Context, ts serverutils.TestServerInterface, cfg *base.Config,
) *rpc.Context {
	var c base.NodeIDContainer
	ctx = logtags.AddTag(ctx, "n", &c)
	rpcContext := rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID:        roachpb.SystemTenantID,
		NodeID:          &c,
		Config:          cfg,
		Clock:           ts.Clock().WallClock(),
		ToleratedOffset: ts.Clock().ToleratedOffset(),
		Stopper:         ts.Stopper(),
		Settings:        ts.ClusterSettings(),
		Knobs:           rpc.ContextTestingKnobs{NoLoopbackDialer: true},
	})
	// Ensure that the RPC client context validates the server cluster ID.
	// This ensures that a test where the server is restarted will not let
	// its test RPC client talk to a server started by an unrelated concurrent test.
	rpcContext.StorageClusterID.Set(ctx, ts.StorageClusterID())
	return rpcContext
}
