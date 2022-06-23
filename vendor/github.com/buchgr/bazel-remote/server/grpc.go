package server

import (
	"context"
	"fmt"
	"net"
	"regexp"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // Register gzip support.
	"google.golang.org/grpc/status"

	asset "github.com/buchgr/bazel-remote/genproto/build/bazel/remote/asset/v1"
	pb "github.com/buchgr/bazel-remote/genproto/build/bazel/remote/execution/v2"
	"github.com/buchgr/bazel-remote/genproto/build/bazel/semver"

	"github.com/buchgr/bazel-remote/cache"
	"github.com/buchgr/bazel-remote/cache/disk"

	_ "github.com/mostynb/go-grpc-compression/snappy" // Register snappy
	_ "github.com/mostynb/go-grpc-compression/zstd"   // and zstd support.
)

const (
	hashKeyLength = 64
	emptySha256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

var (
	// Cache keys must be lower case asciified SHA256 sums.
	hashKeyRegex = regexp.MustCompile("^[a-f0-9]{64}$")
)

type grpcServer struct {
	cache        *disk.Cache
	accessLogger cache.Logger
	errorLogger  cache.Logger
	depsCheck    bool
	mangleACKeys bool
}

// ListenAndServeGRPC creates a new gRPC server and listens on the given
// address. This function either returns an error quickly, or triggers a
// blocking call to https://godoc.org/google.golang.org/grpc#Server.Serve
func ListenAndServeGRPC(addr string, opts []grpc.ServerOption,
	validateACDeps bool,
	mangleACKeys bool,
	enableRemoteAssetAPI bool,
	c *disk.Cache, a cache.Logger, e cache.Logger) error {

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	return serveGRPC(listener, opts, validateACDeps, mangleACKeys, enableRemoteAssetAPI, c, a, e)
}

func serveGRPC(l net.Listener, opts []grpc.ServerOption,
	validateACDepsCheck bool,
	mangleACKeys bool,
	enableRemoteAssetAPI bool,
	c *disk.Cache, a cache.Logger, e cache.Logger) error {

	srv := grpc.NewServer(opts...)
	s := &grpcServer{
		cache: c, accessLogger: a, errorLogger: e,
		depsCheck:    validateACDepsCheck,
		mangleACKeys: mangleACKeys,
	}
	pb.RegisterActionCacheServer(srv, s)
	pb.RegisterCapabilitiesServer(srv, s)
	pb.RegisterContentAddressableStorageServer(srv, s)
	bytestream.RegisterByteStreamServer(srv, s)
	if enableRemoteAssetAPI {
		asset.RegisterFetchServer(srv, s)
	}
	return srv.Serve(l)
}

// Capabilities interface:

func (s *grpcServer) GetCapabilities(ctx context.Context,
	req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {

	// Instance name is currently ignored.

	resp := pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunction: []pb.DigestFunction_Value{pb.DigestFunction_SHA256},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			CachePriorityCapabilities: &pb.PriorityCapabilities{
				Priorities: []*pb.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 0,
					},
				},
			},
			MaxBatchTotalSizeBytes:      0, // "no limit"
			SymlinkAbsolutePathStrategy: pb.SymlinkAbsolutePathStrategy_ALLOWED,
		},
		LowApiVersion:  &semver.SemVer{Major: int32(2)},
		HighApiVersion: &semver.SemVer{Major: int32(2), Minor: int32(1)},
	}

	s.accessLogger.Printf("GRPC GETCAPABILITIES")

	return &resp, nil
}

// Return an error if `hash` is not a valid cache key.
func (s *grpcServer) validateHash(hash string, size int64, logPrefix string) error {
	if size == int64(0) {
		if hash == emptySha256 {
			return nil
		}

		msg := "Invalid zero-length SHA256 hash"
		s.accessLogger.Printf("%s %s: %s", logPrefix, hash, msg)
		return status.Error(codes.InvalidArgument, msg)
	}

	if len(hash) != hashKeyLength {
		msg := fmt.Sprintf("Hash length must be length %d", hashKeyLength)
		s.accessLogger.Printf("%s %s: %s", logPrefix, hash, msg)
		return status.Error(codes.InvalidArgument, msg)
	}

	if !hashKeyRegex.MatchString(hash) {
		msg := "Malformed hash"
		s.accessLogger.Printf("%s %s: %s", logPrefix, hash, msg)
		return status.Error(codes.InvalidArgument, msg)
	}

	return nil
}
