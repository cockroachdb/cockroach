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
	"io"
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
func NewBlobService(basePath string) *Service {
	absPath, _ := filepath.Abs(basePath)
	return &Service{base: absPath}
}

func (s *Service) prependExternalIODir(path string) (string, error) {
	if strings.HasPrefix(path, s.base) {
		return path, nil
	}
	localBase := filepath.Join(s.base, path)
	// Make sure we didn't ../ our way back out.
	if !strings.HasPrefix(localBase, s.base) {
		return "", errors.Errorf("local file access to paths outside of external-io-dir is not allowed")
	}
	return localBase, nil
}

func writeFileLocally(filename string, content io.ReadSeeker) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return errors.Wrap(err, "creating local external storage path")
	}
	tmpP := filename + `.tmp`
	f, err := os.Create(tmpP)
	if err != nil {
		return errors.Wrapf(err, "creating local external tmp file %q", tmpP)
	}
	defer f.Close()
	_, err = io.Copy(f, content)
	if err != nil {
		return errors.Wrapf(err, "writing to local external tmp file %q", tmpP)
	}
	if err := f.Sync(); err != nil {
		return errors.Wrapf(err, "syncing to local external tmp file %q", tmpP)
	}
	return errors.Wrapf(os.Rename(tmpP, filename), "renaming to local export file %q", filename)
}

// Get implements the gRPC service.
func (s *Service) Get(
	ctx context.Context, req *blobspb.GetBlobRequest,
) (*blobspb.GetBlobResponse, error) {
	localFile, err := s.prependExternalIODir(req.Filename)
	if err != nil {
		return nil, err
	}
	payload, err := ioutil.ReadFile(localFile)
	return &blobspb.GetBlobResponse{Payload: payload}, err
}

// Put implements the gRPC service.
func (s *Service) Put(
	ctx context.Context, req *blobspb.PutBlobRequest,
) (*blobspb.PutBlobResponse, error) {
	localFile, err := s.prependExternalIODir(req.Filename)
	if err != nil {
		return nil, err
	}
	return &blobspb.PutBlobResponse{}, writeFileLocally(localFile, bytes.NewReader(req.Payload))
}

// List implements the gRPC service.
func (s *Service) List(
	ctx context.Context, req *blobspb.ListBlobRequest,
) (*blobspb.ListBlobResponse, error) {
	fullPath, err := s.prependExternalIODir(req.Dir)
	if err != nil {
		return nil, err
	}
	matches, err := filepath.Glob(fullPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}
	return &blobspb.ListBlobResponse{Files: matches}, err
}

// Delete implements the gRPC service.
func (s *Service) Delete(
	ctx context.Context, req *blobspb.DeleteBlobRequest,
) (*blobspb.DeleteBlobResponse, error) {
	fullPath, err := s.prependExternalIODir(req.Filename)
	if err != nil {
		return nil, err
	}
	return &blobspb.DeleteBlobResponse{}, os.Remove(fullPath)
}

// Size implements the gRPC service.
func (s *Service) Size(
	ctx context.Context, req *blobspb.GetBlobSizeRequest,
) (*blobspb.GetBlobSizeResponse, error) {
	fullPath, err := s.prependExternalIODir(req.Filename)
	if err != nil {
		return nil, err
	}

	fi, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}
	return &blobspb.GetBlobSizeResponse{Filesize: fi.Size()}, nil
}
