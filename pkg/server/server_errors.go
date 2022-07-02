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
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The functions in this file are responsible for transforming errors
// encountered by API endpoints into something useful to the client
// (and logging them on the way out).
//
// This logic is made extra complicated by the fact the "APIv1" handlers
// are defined as protobuf services, available to HTTP clients
// using grpc-gateway. So we cannot construct HTTP errors directly:
// we have to construct gRPC "*status.Error" objects, which will
// be converted internally by grpc-gateway into HTTP status responses.
//
// In an ideal world, we could make the situation simpler by letting
// the API handlers return errors of any type, and then install
// a gRPC unary interceptor server-side to convert these "any errors"
// into a suitable grpc status.Status.
//
// Unfortunately, we are not in such an ideal world: the gRPC service
// that serves the APIv1 endpoints _also_ serves the KV endpoints and
// other CockroachDB internal APIs. These internal APIs return
// protobuf-encodable error payloads that should be preserved as-is
// for the benefit of internal API clients. So we cannot use an unary
// interceptor to do this error conversion indiscriminately.
//
// We are thus left with the following uncomfortable choice:
//
// - either pre-massage the errors manually into a status.Status
//   inside each API handler (such as done with the current functions
//   defined below).
//
//   The drawback here is that the person implementing an API
//   handler can "forget" to call the error conversion API, with
//   complicated consequences:
//
//   - the payload of the error object flows unredacted to the HTTP
//     client, resulting in a potential data leak. (This is serious)
//   - the grpc status code becomes code.Unknown, which becomes
//     HTTP status 500 at the grpc-gateway boundary. This is sad
//     because oftentimes a different HTTP status would be better
//     suited.
//
// - or, we define a unary interceptor that "does the right thing"
//   by performing redaction of the error object, and compute
//   a grpc status code suitable given the actual error object.
//
//   However, this unary interceptor must then maintain a list
//   of which API endpoints should be given this treatment:
//   all the "internal APIs" should be exclude.
//
// - or, we could pull out the APIv1 tree out of the gRPC system
//   and stop defining it as a protobuf service, and bypass
//   the complexity of grpc-gateway internally.
//
//   This is what we wanted to achieve with the APIv2 tree but
//   unfortunately the APIv1 tree is still lagging behind.
//
// See also: https://github.com/cockroachdb/cockroach/issues/80907
// See also: https://github.com/cockroachdb/cockroach/issues/56208

// newAPIError logs the provided error and converts
// to a gRPC error if it is not one already.
func newAPIError(ctx context.Context, err error) error {
	return newAPIErrorDepth(ctx, 1, codes.Internal, err)
}

func newAPIErrorDepth(ctx context.Context, depth int, code codes.Code, err error) error {
	err = maybeConvertErrorToServerError(err, code)
	// TODO(knz): Also include the API path into the logging output, e.g.
	// by extracting it from the context.
	log.ErrorfDepth(ctx, depth+1, "%v", err)
	return err
}

// handleAPIError calls newAPIError if the err argument is non-nil.
func handleAPIError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	return newAPIErrorDepth(ctx, 1, codes.Internal, err)
}

// newAPIErrorWithCode logs the provided error and converts
// to a gRPC error if it is not one already.
func newAPIErrorWithCode(ctx context.Context, err error, code codes.Code) error {
	err = maybeConvertErrorToServerError(err, code)
	// TODO(knz): Also include the API path into the logging output, e.g.
	// by extracting it from the context.
	log.ErrorfDepth(ctx, 1, "%v", err)
	return err
}

// newAPIErrorf logs the provided error and returns a status.Error
// that should be returned by the RPC endpoint method.
func newAPIErrorf(ctx context.Context, code codes.Code, format string, args ...interface{}) error {
	return newAPIErrorDepthf(ctx, 1, code, format, args...)
}

func newAPIErrorDepthf(
	ctx context.Context, depth int, code codes.Code, format string, args ...interface{},
) error {
	err := errors.NewWithDepthf(depth+1, format, args...)
	return newAPIErrorDepth(ctx, depth+1, code, err)
}

func maybeConvertErrorToServerError(err error, preferredCode codes.Code) error {
	// If it's already a status.Error, assume we've done the work already.
	if _, ok := status.FromError(err); ok {
		return err
	}
	// Try to fish a gRPC status code from the error or one of its
	// causes.
	var gRPCStatuser interface {
		GRPCStatus() *status.Status
	}
	code := preferredCode
	// In some cases, we might want to use the SQL status code to chose
	// a HTTP code.
	sqlStatus := pgerror.GetPGCode(err)
	switch {
	case errors.As(err, &gRPCStatuser):
		newCode := gRPCStatuser.GRPCStatus().Code()
		// The existing status code is not valuable if it's just
		// status.Unknown. In that case, we prefer the preferredCode
		// from the caller.
		if newCode != codes.Unknown {
			code = newCode
		}
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.HasAssertionFailure(err):
		code = codes.Internal
	case isNotFoundError(err):
		code = codes.NotFound
	default:
		switch sqlStatus {
		case pgcode.UndefinedDatabase, pgcode.UndefinedSchema, pgcode.UndefinedTable, pgcode.UndefinedObject:
			code = codes.NotFound
		}
	}

	// Provide a redacted version of the error to the user.
	var buf redact.StringBuilder
	buf.Print(err)
	prefix := ""
	suffix := ""
	msg := buf.RedactableString().Redact().StripMarkers()

	// Add a generic prefix if the error is "serious" or undetermined.
	if code == codes.Internal || code == codes.Unknown {
		prefix = "An internal server error has occurred. Check your CockroachDB logs for more details.\n"
	}
	// Add the SQLSTATE if there's one.
	if sqlStatus != pgcode.Uncategorized {
		suffix = fmt.Sprintf(" (SQLSTATE %s)", sqlStatus)
	}

	return status.Error(code, prefix+msg+suffix)
}

// errRequiresAdmin constructs an error that informs the user they
// need to have the admin role.
func errRequiresAdmin(ctx context.Context) error {
	// We use a constructor instead of pre-allocating the error to
	// ensure we capture the stack trace in investigations.
	return newAPIErrorDepthf(ctx, 1, codes.PermissionDenied, "this operation requires admin privilege")
}

func errRequiresRoleOption(ctx context.Context, option roleoption.Option) error {
	return newAPIErrorDepthf(ctx, 1, codes.PermissionDenied, "this operation requires %s privilege", option)
}

// isNotFoundError returns true if err is a table/database not found error.
func isNotFoundError(err error) bool {
	// TODO(cdo): Replace this crude suffix-matching with something more structured once we have
	// more structured errors.
	return err != nil && strings.HasSuffix(err.Error(), "does not exist")
}

// httpSendError packages the given error using the standard API
// path and sends it to the HTTP response directly.
// This can be used on paths that do not use grpc-gateway,
// such as in the APIv2 tree.
func httpSendError(ctx context.Context, err error, w http.ResponseWriter) {
	err = newAPIErrorDepth(ctx, 1, codes.Internal, err)
	grpcStatusCode := status.Code(err)
	http.Error(w, err.Error(), runtime.HTTPStatusFromCode(grpcStatusCode))
}
