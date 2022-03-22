// Copyright 2021 The Cockroach Authors.
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
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// profileLocal runs a performance profile of the requested type (heap, cpu etc).
// on the local node. This method returns a gRPC error to the caller.
func profileLocal(
	ctx context.Context, req *serverpb.ProfileRequest, st *cluster.Settings,
) (*serverpb.JSONResponse, error) {
	switch req.Type {
	case serverpb.ProfileRequest_CPU:
		var buf bytes.Buffer
		profileType := cluster.CPUProfileDefault
		if req.Labels {
			profileType = cluster.CPUProfileWithLabels
		}
		if err := debug.CPUProfileDo(st, profileType, func() error {
			duration := 30 * time.Second
			if req.Seconds != 0 {
				duration = time.Duration(req.Seconds) * time.Second
			}
			if err := pprof.StartCPUProfile(&buf); err != nil {
				// Construct a gRPC error to return to the caller.
				return serverError(ctx, err)
			}
			defer pprof.StopCPUProfile()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(duration):
				return nil
			}
		}); err != nil {
			return nil, err
		}
		return &serverpb.JSONResponse{Data: buf.Bytes()}, nil
	default:
		name, ok := serverpb.ProfileRequest_Type_name[int32(req.Type)]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown profile: %d", req.Type)
		}
		name = strings.ToLower(name)
		p := pprof.Lookup(name)
		if p == nil {
			return nil, status.Errorf(codes.InvalidArgument, "unable to find profile: %s", name)
		}
		var buf bytes.Buffer
		if err := p.WriteTo(&buf, 0); err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		return &serverpb.JSONResponse{Data: buf.Bytes()}, nil
	}
}

// stacksLocal retrieves goroutine stack files on the local node. This method
// returns a gRPC error to the caller.
func stacksLocal(req *serverpb.StacksRequest) (*serverpb.JSONResponse, error) {
	var stackType int
	switch req.Type {
	case serverpb.StacksType_GOROUTINE_STACKS:
		stackType = 2
	case serverpb.StacksType_GOROUTINE_STACKS_DEBUG_1:
		stackType = 1
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown stacks type: %s", req.Type)
	}

	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, stackType); err != nil {
		return nil, status.Errorf(codes.Unknown, "failed to write goroutine stack: %s", err)
	}
	return &serverpb.JSONResponse{Data: buf.Bytes()}, nil
}

// getLocalFiles retrieves the requested files for the local node. This method
// returns a gRPC error to the caller.
func getLocalFiles(
	req *serverpb.GetFilesRequest, heapProfileDirName string, goroutineDumpDirName string,
) (*serverpb.GetFilesResponse, error) {
	var dir string
	switch req.Type {
	// TODO(ridwanmsharif): Serve logfiles so debug-zip can fetch them
	// instead of reading individual entries.
	case serverpb.FileType_HEAP: // Requesting for saved Heap Profiles.
		dir = heapProfileDirName
	case serverpb.FileType_GOROUTINES: // Requesting for saved Goroutine dumps.
		dir = goroutineDumpDirName
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown file type: %s", req.Type)
	}
	if dir == "" {
		return nil, status.Errorf(codes.Unimplemented, "dump directory not configured: %s", req.Type)
	}
	var resp serverpb.GetFilesResponse
	for _, pattern := range req.Patterns {
		if err := checkFilePattern(pattern); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		filepaths, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad pattern: %s", pattern)
		}

		for _, path := range filepaths {
			fileinfo, _ := os.Stat(path)
			var contents []byte
			if !req.ListOnly {
				contents, err = ioutil.ReadFile(path)
				if err != nil {
					return nil, status.Errorf(codes.Internal, err.Error())
				}
			}
			resp.Files = append(resp.Files,
				&serverpb.File{Name: fileinfo.Name(), FileSize: fileinfo.Size(), Contents: contents})
		}
	}
	return &resp, nil
}
