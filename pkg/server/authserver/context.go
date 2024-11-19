// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package authserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc/metadata"
)

type webSessionUserKey struct{}
type webSessionIDKey struct{}

const webSessionUserKeyStr = "websessionuser"
const webSessionIDKeyStr = "websessionid"

// ContextWithHTTPAuthInfo embeds the HTTP authentication details into
// a go context. Meant for use with userFromHTTPAuthInfoContext().
func ContextWithHTTPAuthInfo(
	ctx context.Context, username string, sessionID int64,
) context.Context {
	ctx = context.WithValue(ctx, webSessionUserKey{}, username)
	if sessionID != 0 {
		ctx = context.WithValue(ctx, webSessionIDKey{}, sessionID)
	}
	return ctx
}

// UserFromHTTPAuthInfoContext returns a SQL username from the request
// context of a HTTP route requiring login. Only use in routes that require
// login (e.g. requiresAuth = true in the API v2 route definition).
//
// Do not use this function in _RPC_ API handlers. These access their
// SQL identity via the RPC incoming context. See
// userFromIncomingRPCContext().
func UserFromHTTPAuthInfoContext(ctx context.Context) username.SQLUsername {
	return username.MakeSQLUsernameFromPreNormalizedString(ctx.Value(webSessionUserKey{}).(string))
}

// MaybeUserFromHTTPAuthInfoContext is like userFromHTTPAuthInfoContext but
// it returns a boolean false if there is no user in the context.
func MaybeUserFromHTTPAuthInfoContext(ctx context.Context) (username.SQLUsername, bool) {
	if u := ctx.Value(webSessionUserKey{}); u != nil {
		return username.MakeSQLUsernameFromPreNormalizedString(u.(string)), true
	}
	return username.SQLUsername{}, false
}

// TranslateHTTPAuthInfoToGRPCMetadata translates the context.Value
// that results from HTTP authentication into gRPC metadata suitable
// for use by RPC API handlers.
func TranslateHTTPAuthInfoToGRPCMetadata(ctx context.Context, _ *http.Request) metadata.MD {
	md := metadata.MD{}
	if user := ctx.Value(webSessionUserKey{}); user != nil {
		md.Set(webSessionUserKeyStr, user.(string))
	}
	if sessionID := ctx.Value(webSessionIDKey{}); sessionID != nil {
		md.Set(webSessionIDKeyStr, fmt.Sprintf("%v", sessionID))
	}
	return md
}

// ForwardSQLIdentityThroughRPCCalls forwards the SQL identity of the
// original request (as populated by translateHTTPAuthInfoToGRPCMetadata in
// grpc-gateway) so it remains available to the remote node handling
// the request.
func ForwardSQLIdentityThroughRPCCalls(ctx context.Context) context.Context {
	if md, ok := grpcutil.FastFromIncomingContext(ctx); ok {
		if u, ok := md[webSessionUserKeyStr]; ok {
			return metadata.NewOutgoingContext(ctx, metadata.MD{webSessionUserKeyStr: u})
		}
	}
	return ctx
}

// ForwardHTTPAuthInfoToRPCCalls converts an HTTP API (v1 or v2) context, to one that
// can issue outgoing RPC requests under the same logged-in user.
func ForwardHTTPAuthInfoToRPCCalls(ctx context.Context, r *http.Request) context.Context {
	md := TranslateHTTPAuthInfoToGRPCMetadata(ctx, r)
	return metadata.NewOutgoingContext(ctx, md)
}

// UserFromIncomingRPCContext is to be used in RPC API handlers. It
// assumes the SQL identity was populated in the context implicitly by
// gRPC via translateHTTPAuthInfoToGRPCMetadata(), or explicitly via
// forwardHTTPAuthInfoToRPCCalls() or
// forwardSQLIdentityThroughRPCCalls().
//
// Do not use this function in _HTTP_ API handlers. Those access their
// SQL identity via a special context key. See
// userFromHTTPAuthInfoContext().
func UserFromIncomingRPCContext(ctx context.Context) (res username.SQLUsername, err error) {
	md, ok := grpcutil.FastFromIncomingContext(ctx)
	if !ok {
		return username.RootUserName(), nil
	}
	usernames, ok := md[webSessionUserKeyStr]
	if !ok {
		// If the incoming context has metadata but no attached web session user,
		// it's a gRPC / internal SQL connection which has root on the cluster.
		// This assumption is a historical hiccup, and would be best described
		// as a bug. See: https://github.com/cockroachdb/cockroach/issues/45018
		return username.RootUserName(), nil
	}
	if len(usernames) != 1 {
		log.Warningf(ctx, "context's incoming metadata contains unexpected number of usernames: %+v ", md)
		return res, fmt.Errorf(
			"context's incoming metadata contains unexpected number of usernames: %+v ", md)
	}
	// At this point the user is already logged in, so we can assume
	// the username has been normalized already.
	username := username.MakeSQLUsernameFromPreNormalizedString(usernames[0])
	return username, nil
}
