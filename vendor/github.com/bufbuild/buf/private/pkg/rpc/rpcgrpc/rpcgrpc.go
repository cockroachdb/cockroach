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

package rpcgrpc

import (
	"context"
	"strings"

	"github.com/bufbuild/buf/private/pkg/rpc"
	"github.com/bufbuild/buf/private/pkg/rpc/rpcheader"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	errorCodeToGRPCCode = map[rpc.ErrorCode]codes.Code{
		rpc.ErrorCodeCanceled:           codes.Canceled,
		rpc.ErrorCodeUnknown:            codes.Unknown,
		rpc.ErrorCodeInvalidArgument:    codes.InvalidArgument,
		rpc.ErrorCodeDeadlineExceeded:   codes.DeadlineExceeded,
		rpc.ErrorCodeNotFound:           codes.NotFound,
		rpc.ErrorCodeAlreadyExists:      codes.AlreadyExists,
		rpc.ErrorCodePermissionDenied:   codes.PermissionDenied,
		rpc.ErrorCodeResourceExhausted:  codes.ResourceExhausted,
		rpc.ErrorCodeFailedPrecondition: codes.FailedPrecondition,
		rpc.ErrorCodeAborted:            codes.Aborted,
		rpc.ErrorCodeOutOfRange:         codes.OutOfRange,
		rpc.ErrorCodeUnimplemented:      codes.Unimplemented,
		rpc.ErrorCodeInternal:           codes.Internal,
		rpc.ErrorCodeUnavailable:        codes.Unavailable,
		rpc.ErrorCodeDataLoss:           codes.DataLoss,
		rpc.ErrorCodeUnauthenticated:    codes.Unauthenticated,
	}

	grpcCodeToErrorCode = map[codes.Code]rpc.ErrorCode{
		codes.Canceled:           rpc.ErrorCodeCanceled,
		codes.Unknown:            rpc.ErrorCodeUnknown,
		codes.InvalidArgument:    rpc.ErrorCodeInvalidArgument,
		codes.DeadlineExceeded:   rpc.ErrorCodeDeadlineExceeded,
		codes.NotFound:           rpc.ErrorCodeNotFound,
		codes.AlreadyExists:      rpc.ErrorCodeAlreadyExists,
		codes.PermissionDenied:   rpc.ErrorCodePermissionDenied,
		codes.ResourceExhausted:  rpc.ErrorCodeResourceExhausted,
		codes.FailedPrecondition: rpc.ErrorCodeFailedPrecondition,
		codes.Aborted:            rpc.ErrorCodeAborted,
		codes.OutOfRange:         rpc.ErrorCodeOutOfRange,
		codes.Unimplemented:      rpc.ErrorCodeUnimplemented,
		codes.Internal:           rpc.ErrorCodeInternal,
		codes.Unavailable:        rpc.ErrorCodeUnavailable,
		codes.DataLoss:           rpc.ErrorCodeDataLoss,
		codes.Unauthenticated:    rpc.ErrorCodeUnauthenticated,
	}
)

// NewUnaryServerInterceptor returns a new UnaryServerInterceptor.
//
// This should be the last interceptor installed.
func NewUnaryServerInterceptor(serverInterceptors ...rpc.ServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		request interface{},
		unaryServerInfo *grpc.UnaryServerInfo,
		unaryHandler grpc.UnaryHandler,
	) (_ interface{}, retErr error) {
		defer func() {
			retErr = toGRPCError(retErr)
		}()

		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = rpc.WithIncomingHeaders(ctx, fromGRPCMetadata(md))
			// make sure that no one is mistakenly using grpc metadata
			ctx = metadata.NewIncomingContext(ctx, nil)
		}

		serverHandler := rpc.ServerHandlerFunc(unaryHandler)
		if serverInterceptor := rpc.NewChainedServerInterceptor(serverInterceptors...); serverInterceptor != nil {
			return serverInterceptor.Intercept(ctx, request, toServerInfo(unaryServerInfo), serverHandler)
		}
		return serverHandler.Handle(ctx, request)
	}
}

// NewStreamServerInterceptor returns a new StreamServerInterceptor.
//
// This should be the last interceptor installed.
func NewStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		serverStream grpc.ServerStream,
		streamServerInfo *grpc.StreamServerInfo,
		streamHandler grpc.StreamHandler,
	) error {
		return status.Error(codes.Internal, "streaming not supported")
	}
}

// NewUnaryClientInterceptor returns a new UnaryClientInterceptor.
//
// This should be the last interceptor installed.
func NewUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		request interface{},
		response interface{},
		clientConn *grpc.ClientConn,
		unaryInvoker grpc.UnaryInvoker,
		callOptions ...grpc.CallOption,
	) error {
		if headers := rpc.GetOutgoingHeaders(ctx); len(headers) > 0 {
			ctx = metadata.NewOutgoingContext(ctx, toGRPCMetadata(headers))
		}
		return fromGRPCError(unaryInvoker(ctx, method, request, response, clientConn, callOptions...))
	}
}

// NewStreamClientInterceptor returns a new StreamClientInterceptor.
//
// This should be the last interceptor installed.
func NewStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		streamDesc *grpc.StreamDesc,
		clientConn *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		callOptions ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		return nil, rpc.NewInternalError("streaming not supported")
	}
}

func toGRPCMetadata(headers map[string]string) metadata.MD {
	metadataHeaders := make(map[string]string, len(headers))
	for key, value := range headers {
		metadataHeaders[rpcheader.KeyPrefix+key] = value
	}
	return metadata.New(metadataHeaders)
}

func fromGRPCMetadata(md metadata.MD) map[string]string {
	headers := make(map[string]string)
	for key, values := range md {
		key = strings.ToLower(key)
		// prefix so that we strip out other headers
		// rpc clients and servers should only be aware of headers set with the rpc package
		if strings.HasPrefix(key, rpcheader.KeyPrefix) {
			if key := strings.TrimPrefix(key, rpcheader.KeyPrefix); key != "" {
				headers[key] = values[0]
			}
		}
	}
	return headers
}

// toGRPCError converts the error to a GRPC error.
//
// If the err is nil, this returns nil.
// If the error is already a GRPC Error, err is returned.
// Otherwise, this is converted to a rpc Error, and then to a GRPC Error.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	errorCode := rpc.GetErrorCode(err)
	grpcCode, ok := errorCodeToGRPCCode[errorCode]
	if !ok {
		grpcCode = codes.Internal
	}
	return status.Error(grpcCode, err.Error())
}

// fromGRPCError converts the potential GRPC error to an Error.
//
// If the err is nil, this returns nil.
// If the error is already a rpc Error, err is returned.
// Otherwise, this is converted to a GRPC Error, and then to a rpc Error.
func fromGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if rpc.IsError(err) {
		return err
	}
	st, ok := status.FromError(err)
	if !ok {
		st = status.New(codes.Internal, err.Error())
	}
	if st.Code() == codes.OK {
		return nil
	}
	errorCode, ok := grpcCodeToErrorCode[st.Code()]
	if !ok {
		errorCode = rpc.ErrorCodeInternal
	}
	return rpc.NewError(errorCode, st.Message())
}

func toServerInfo(unaryServerInfo *grpc.UnaryServerInfo) *rpc.ServerInfo {
	if unaryServerInfo == nil {
		return &rpc.ServerInfo{}
	}
	return &rpc.ServerInfo{
		Path: unaryServerInfo.FullMethod,
	}
}
