// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpctwirp

import (
	"context"

	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/twitchtv/twirp"
)

var (
	errorCodeToTwirpErrorCode = map[rpc.ErrorCode]twirp.ErrorCode{
		rpc.ErrorCodeCanceled:           twirp.Canceled,
		rpc.ErrorCodeUnknown:            twirp.Unknown,
		rpc.ErrorCodeInvalidArgument:    twirp.InvalidArgument,
		rpc.ErrorCodeDeadlineExceeded:   twirp.DeadlineExceeded,
		rpc.ErrorCodeNotFound:           twirp.NotFound,
		rpc.ErrorCodeAlreadyExists:      twirp.AlreadyExists,
		rpc.ErrorCodePermissionDenied:   twirp.PermissionDenied,
		rpc.ErrorCodeResourceExhausted:  twirp.ResourceExhausted,
		rpc.ErrorCodeFailedPrecondition: twirp.FailedPrecondition,
		rpc.ErrorCodeAborted:            twirp.Aborted,
		rpc.ErrorCodeOutOfRange:         twirp.OutOfRange,
		rpc.ErrorCodeUnimplemented:      twirp.Unimplemented,
		rpc.ErrorCodeInternal:           twirp.Internal,
		rpc.ErrorCodeUnavailable:        twirp.Unavailable,
		rpc.ErrorCodeDataLoss:           twirp.DataLoss,
		rpc.ErrorCodeUnauthenticated:    twirp.Unauthenticated,
	}

	twirpErrorCodeToErrorCode = map[twirp.ErrorCode]rpc.ErrorCode{
		twirp.Canceled:           rpc.ErrorCodeCanceled,
		twirp.Unknown:            rpc.ErrorCodeUnknown,
		twirp.InvalidArgument:    rpc.ErrorCodeInvalidArgument,
		twirp.Malformed:          rpc.ErrorCodeInvalidArgument,
		twirp.DeadlineExceeded:   rpc.ErrorCodeDeadlineExceeded,
		twirp.NotFound:           rpc.ErrorCodeNotFound,
		twirp.BadRoute:           rpc.ErrorCodeNotFound,
		twirp.AlreadyExists:      rpc.ErrorCodeAlreadyExists,
		twirp.PermissionDenied:   rpc.ErrorCodePermissionDenied,
		twirp.ResourceExhausted:  rpc.ErrorCodeResourceExhausted,
		twirp.FailedPrecondition: rpc.ErrorCodeFailedPrecondition,
		twirp.Aborted:            rpc.ErrorCodeAborted,
		twirp.OutOfRange:         rpc.ErrorCodeOutOfRange,
		twirp.Unimplemented:      rpc.ErrorCodeUnimplemented,
		twirp.Internal:           rpc.ErrorCodeInternal,
		twirp.Unavailable:        rpc.ErrorCodeUnavailable,
		twirp.DataLoss:           rpc.ErrorCodeDataLoss,
		twirp.Unauthenticated:    rpc.ErrorCodeUnauthenticated,
	}
)

// NewServerInterceptor returns a new server Interceptor for twirp.
//
// This should be the last interceptor installed.
func NewServerInterceptor(serverInterceptors ...rpc.ServerInterceptor) twirp.Interceptor {
	return func(next twirp.Method) twirp.Method {
		return func(ctx context.Context, request interface{}) (_ interface{}, retErr error) {
			defer func() {
				retErr = toTwirpError(retErr)
			}()
			serverHandler := rpc.ServerHandlerFunc(next)
			if serverInterceptor := rpc.NewChainedServerInterceptor(serverInterceptors...); serverInterceptor != nil {
				return serverInterceptor.Intercept(ctx, request, toServerInfo(ctx), serverHandler)
			}
			return serverHandler.Handle(ctx, request)
		}
	}
}

// NewClientInterceptor returns a new client Interceptor for twirp.
//
// This should be the last interceptor installed.
func NewClientInterceptor() twirp.Interceptor {
	return func(next twirp.Method) twirp.Method {
		return func(ctx context.Context, request interface{}) (interface{}, error) {
			response, err := next(ctx, request)
			return response, fromTwirpError(err)
		}
	}
}

// toTwirpError converts the error to a Twirp error.
//
// If the err is nil, this returns nil.
// If the error is already a Twirp error, err is returned.
// Otherwise, this is converted to a rpc error, and then to a Twirp error.
func toTwirpError(err error) error {
	if err == nil {
		return nil
	}
	twirpError, ok := err.(twirp.Error)
	if ok {
		return twirpError
	}
	errorCode := rpc.GetErrorCode(err)
	twirpErrorCode, ok := errorCodeToTwirpErrorCode[errorCode]
	if !ok {
		twirpErrorCode = twirp.Internal
	}
	return twirp.NewError(twirpErrorCode, err.Error())
}

// fromTwirpError converts the potential Twirp error to an Error.
//
// If the err is nil, this returns nil.
// If the error is already a rpc Error, err is returned.
// Otherwise, this is converted to a Twirp error, and then to a rpc error.
func fromTwirpError(err error) error {
	if err == nil {
		return nil
	}
	if rpc.IsError(err) {
		return err
	}
	twirpError, ok := err.(twirp.Error)
	if !ok {
		twirpError = twirp.NewError(twirp.Internal, err.Error())
	}
	if twirpError.Code() == twirp.NoError {
		return nil
	}
	errorCode, ok := twirpErrorCodeToErrorCode[twirpError.Code()]
	if !ok {
		errorCode = rpc.ErrorCodeInternal
	}
	return rpc.NewError(errorCode, twirpError.Msg())
}

func toServerInfo(ctx context.Context) *rpc.ServerInfo {
	pkgName, _ := twirp.PackageName(ctx)
	shortServiceName, _ := twirp.ServiceName(ctx)
	serviceName := shortServiceName
	if pkgName != "" && shortServiceName != "" {
		serviceName = pkgName + "." + shortServiceName
	}
	methodName, _ := twirp.MethodName(ctx)
	if serviceName != "" && methodName != "" {
		return &rpc.ServerInfo{
			Path: "/" + serviceName + "/" + methodName,
		}
	}
	return &rpc.ServerInfo{}
}
