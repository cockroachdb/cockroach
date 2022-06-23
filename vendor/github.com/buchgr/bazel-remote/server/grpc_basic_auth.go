package server

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpc_status "google.golang.org/grpc/status"

	auth "github.com/abbot/go-http-auth"
)

var (
	errNoMetadata = grpc_status.Error(codes.Unauthenticated,
		"no metadata found")
	errNoAuthMetadata = grpc_status.Error(codes.Unauthenticated,
		"no authentication metadata found")
	errAccessDenied = grpc_status.Error(codes.Unauthenticated,
		"access denied")
)

// GrpcBasicAuth wraps an auth.SecretProvider, and provides gRPC interceptors
// that verify that requests can be authenticated using HTTP basic auth.
type GrpcBasicAuth struct {
	secrets auth.SecretProvider
}

// NewGrpcBasicAuth returns a GrpcBasicAuth that wraps the given
// auth.SecretProvider.
func NewGrpcBasicAuth(secrets auth.SecretProvider) *GrpcBasicAuth {
	return &GrpcBasicAuth{secrets: secrets}
}

// StreamServerInterceptor returns a streaming server interceptor that
// verifies that each request can be authenticated using HTTP basic auth.
func (b *GrpcBasicAuth) StreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	username, password, err := getLogin(ss.Context())
	if err != nil {
		return err
	}
	if username == "" || password == "" {
		return errAccessDenied
	}

	if !b.allowed(username, password) {
		return errAccessDenied
	}

	return handler(srv, ss)
}

// UnaryServerInterceptor returns a unary server interceptor that verifies
// that each request can be authenticated using HTTP basic auth.
func (b *GrpcBasicAuth) UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	username, password, err := getLogin(ctx)
	if err != nil {
		return nil, err
	}
	if username == "" || password == "" {
		return nil, errAccessDenied
	}

	if !b.allowed(username, password) {
		return nil, errAccessDenied
	}

	return handler(ctx, req)
}

func getLogin(ctx context.Context) (username, password string, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", errNoMetadata
	}

	for k, v := range md {
		if k == ":authority" && len(v) > 0 {
			// When bazel is run with --remote_cache=grpc://user:pass@address/"
			// the value looks like "user:pass@address".
			fields := strings.SplitN(v[0], ":", 2)
			if len(fields) < 2 {
				continue
			}
			username = fields[0]

			fields = strings.SplitN(fields[1], "@", 2)
			if len(fields) < 2 {
				continue
			}
			password = fields[0]

			return username, password, nil
		}
	}

	return "", "", errNoAuthMetadata
}

func (b *GrpcBasicAuth) allowed(username, password string) bool {
	ignoredRealm := ""
	requiredSecret := b.secrets(username, ignoredRealm)
	if requiredSecret == "" {
		return false // User does not exist.
	}

	return auth.CheckSecret(password, requiredSecret)
}
