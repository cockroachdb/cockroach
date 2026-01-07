// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/debug/pprofui"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/google/pprof/profile"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// profileLocal runs a performance profile of the requested type (heap, cpu etc).
// on the local node. This method returns a gRPC error to the caller.
func profileLocal(
	ctx context.Context, req *serverpb.ProfileRequest, st *cluster.Settings, nodeID roachpb.NodeID,
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
				return srverrors.ServerError(ctx, err)
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
	case serverpb.ProfileRequest_GOROUTINE:
		p := pprof.Lookup("goroutine")
		if p == nil {
			return nil, status.Error(codes.Internal, "unable to find goroutine profile")
		}
		var buf bytes.Buffer
		if req.Labels {
			buf.WriteString(fmt.Sprintf("Stacks for node: %d\n\n", nodeID))
			if err := p.WriteTo(&buf, 1); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			buf.WriteString("\n\n")

			// Now check if we need to filter the goroutines by the provided label filter.
			if req.LabelFilter != "" {
				return &serverpb.JSONResponse{Data: pprofui.FilterStacksWithLabels(buf.Bytes(), req.LabelFilter)}, nil
			}
		} else {
			if err := p.WriteTo(&buf, 0); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
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

		// If seconds is specified, collect a delta profile by taking two snapshots
		// and computing the difference. This matches the behavior of Go's standard
		// net/http/pprof handler.
		if req.Seconds > 0 {
			return collectDeltaProfile(ctx, p, time.Duration(req.Seconds)*time.Second)
		}

		var buf bytes.Buffer
		if err := p.WriteTo(&buf, 0); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &serverpb.JSONResponse{Data: buf.Bytes()}, nil
	}
}

// collectDeltaProfile collects a delta profile by taking two snapshots of the
// given profile separated by the specified duration, then computing the
// difference. This matches the behavior of Go's standard net/http/pprof handler
// when the "seconds" parameter is specified.
func collectDeltaProfile(
	ctx context.Context, p *pprof.Profile, duration time.Duration,
) (*serverpb.JSONResponse, error) {
	// Collect the first profile snapshot.
	p0, err := collectProfileSnapshot(p)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to collect initial profile: %s", err)
	}

	// Wait for the specified duration.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(duration):
	}

	// Collect the second profile snapshot.
	p1, err := collectProfileSnapshot(p)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to collect final profile: %s", err)
	}

	// Compute the delta by scaling p0 by -1 and merging with p1.
	ts := p1.TimeNanos
	dur := p1.TimeNanos - p0.TimeNanos

	p0.Scale(-1)

	delta, err := profile.Merge([]*profile.Profile{p0, p1})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to compute delta profile: %s", err)
	}

	delta.TimeNanos = ts
	delta.DurationNanos = dur

	var buf bytes.Buffer
	if err := delta.Write(&buf); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write delta profile: %s", err)
	}
	return &serverpb.JSONResponse{Data: buf.Bytes()}, nil
}

// collectProfileSnapshot collects a single snapshot of the given profile and
// parses it into a *profile.Profile.
func collectProfileSnapshot(p *pprof.Profile) (*profile.Profile, error) {
	var buf bytes.Buffer
	if err := p.WriteTo(&buf, 0); err != nil {
		return nil, err
	}
	ts := timeutil.Now().UnixNano()
	prof, err := profile.Parse(&buf)
	if err != nil {
		return nil, err
	}
	prof.TimeNanos = ts
	return prof, nil
}

// stacksLocal retrieves goroutine stack files on the local node. This method
// returns a gRPC error to the caller.
func stacksLocal(req *serverpb.StacksRequest) (*serverpb.JSONResponse, error) {
	var buf *bytes.Buffer
	switch req.Type {
	case serverpb.StacksType_GOROUTINE_STACKS:
		// Use allstacks.Get because it doesn't have an arbitrary 64MB limit (as
		// opposed to the profile).
		buf = bytes.NewBuffer(allstacks.Get())
	case serverpb.StacksType_GOROUTINE_STACKS_DEBUG_1:
		buf = bytes.NewBuffer(nil)
		if err := pprof.Lookup("goroutine").WriteTo(buf, 1 /* debug */); err != nil {
			return nil, status.Errorf(codes.Unknown, "failed to write goroutine stack: %s", err)
		}
	case serverpb.StacksType_GOROUTINE_STACKS_DEBUG_3:
		buf = bytes.NewBuffer(nil)
		if err := pprof.Lookup("goroutine").WriteTo(buf, 3 /* debug */); err != nil {
			return nil, status.Errorf(codes.Unknown, "failed to write goroutine stack: %s", err)
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown stacks type: %s", req.Type)
	}
	return &serverpb.JSONResponse{Data: buf.Bytes()}, nil
}

// getLocalFiles retrieves the requested files for the local node. This method
// returns a gRPC error to the caller.
func getLocalFiles(
	req *serverpb.GetFilesRequest,
	heapProfileDirName string,
	goroutineDumpDirName string,
	cpuProfileDirName string,
	executionTraceDirName string,
	statFileFn func(string) (os.FileInfo, error),
	readFileFn func(string) ([]byte, error),
) (*serverpb.GetFilesResponse, error) {
	var dir string
	switch req.Type {
	// TODO(ridwanmsharif): Serve logfiles so debug-zip can fetch them
	// instead of reading individual entries.
	case serverpb.FileType_HEAP: // Requesting for saved Heap Profiles.
		dir = heapProfileDirName
	case serverpb.FileType_GOROUTINES: // Requesting for saved Goroutine dumps.
		dir = goroutineDumpDirName
	case serverpb.FileType_CPU: // Requesting for saved CPU Profiles.
		dir = cpuProfileDirName
	case serverpb.FileType_EXECUTIONTRACE:
		dir = executionTraceDirName
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown file type: %s", req.Type)
	}
	if dir == "" {
		return nil, status.Errorf(codes.Unimplemented, "dump directory not configured: %s", req.Type)
	}
	var resp serverpb.GetFilesResponse
	for _, pattern := range req.Patterns {
		if err := checkFilePattern(pattern); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		filepaths, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad pattern: %s", pattern)
		}

		for _, path := range filepaths {
			fileinfo, err := statFileFn(path)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			var contents []byte
			if !req.ListOnly {
				contents, err = readFileFn(path)
				if err != nil {
					return nil, status.Error(codes.Internal, err.Error())
				}
			}
			resp.Files = append(resp.Files,
				&serverpb.File{Name: fileinfo.Name(), FileSize: fileinfo.Size(), Contents: contents})
		}
	}
	return &resp, nil
}
