// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blobs

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/pkg/errors"
)

// Service implements the gRPC BlobService which exchanges bulk files between different nodes.
type Service struct {
	base string
}

var _ blobspb.BlobServer = &Service{}

// NewBlobService instantiates a blob service server.
func NewBlobService(basePath string) (*Service, error) {
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, errors.Wrap(err, "externalIODir should be an absolute path")
	}
	return &Service{base: absPath}, nil
}

func (s *Service) prependExternalIODir(path string) (string, error) {
	localBase := filepath.Join(s.base, path)
	// Make sure we didn't ../ our way back out.
	if !strings.HasPrefix(localBase, s.base) {
		return "", errors.Errorf("local file access to paths outside of external-io-dir is not allowed")
	}
	return localBase, nil
}

// GetBlob implements the gRPC service.
func (s *Service) GetBlob(
	ctx context.Context, req *blobspb.GetRequest,
) (*blobspb.GetResponse, error) {
	localFile, err := s.prependExternalIODir(req.Filename)
	if err != nil {
		return nil, err
	}
	payload, err := ioutil.ReadFile(localFile)
	return &blobspb.GetResponse{Payload: payload}, err
}

// PutBlob implements the gRPC service.
func (s *Service) PutBlob(
	ctx context.Context, req *blobspb.PutRequest,
) (*blobspb.PutResponse, error) {
	localFile, err := s.prependExternalIODir(req.Filename)
	if err != nil {
		return nil, err
	}
	return &blobspb.PutResponse{}, writeFileLocally(localFile, bytes.NewReader(req.Payload))
}

// List implements the gRPC service.
func (s *Service) List(
	ctx context.Context, req *blobspb.GlobRequest,
) (*blobspb.GlobResponse, error) {
	fullPath, err := s.prependExternalIODir(req.Pattern)
	if err != nil {
		return nil, err
	}
	matches, err := filepath.Glob(fullPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}
	return &blobspb.GlobResponse{Files: matches}, err
}

// Delete implements the gRPC service.
func (s *Service) Delete(
	ctx context.Context, req *blobspb.DeleteRequest,
) (*blobspb.DeleteResponse, error) {
	fullPath, err := s.prependExternalIODir(req.Filename)
	if err != nil {
		return nil, err
	}
	return &blobspb.DeleteResponse{}, os.Remove(fullPath)
}

// Stat implements the gRPC service.
func (s *Service) Stat(ctx context.Context, req *blobspb.StatRequest) (*blobspb.BlobStat, error) {
	fullPath, err := s.prependExternalIODir(req.Filename)
	if err != nil {
		return nil, err
	}

	fi, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, errors.Errorf("expected a file but %q is a directory", fi.Name())
	}
	return &blobspb.BlobStat{Filesize: fi.Size()}, nil
}
