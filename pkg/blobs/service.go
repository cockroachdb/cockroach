// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package blobs contains a gRPC service to be used for remote file access.

It is used for bulk file reads and writes to files on any CockroachDB node.
Each node will run a blob service, which serves the file access for files on
that node. Each node will also have a blob client, which uses the nodedialer
to connect to another node's blob service, and access its files. The blob client
is the point of entry to this service and it supports the `BlobClient` interface,
which includes the following functionalities:
  - ReadFile
  - WriteFile
  - List
  - Delete
  - Stat
*/
package blobs

import (
	"bytes"
	"context"
	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
)

// Service implements the gRPC BlobService which exchanges bulk files between different nodes.
type Service struct {
	localStorage *localStorage
}

var _ blobspb.BlobServer = &Service{}

// NewBlobService instantiates a blob service server.
func NewBlobService(externalIODir string) (*Service, error) {
	localStorage, err := newLocalStorage(externalIODir)
	return &Service{localStorage: localStorage}, err
}

// GetBlob implements the gRPC service.
func (s *Service) GetBlob(
	ctx context.Context, req *blobspb.GetRequest,
) (*blobspb.GetResponse, error) {
	reader, err := s.localStorage.ReadFile(req.Filename)
	if err != nil {
		return &blobspb.GetResponse{}, err
	}
	payload, err := ioutil.ReadAll(reader)
	return &blobspb.GetResponse{Payload: payload}, err
}

// PutBlob implements the gRPC service.
func (s *Service) PutBlob(
	ctx context.Context, req *blobspb.PutRequest,
) (*blobspb.PutResponse, error) {
	return &blobspb.PutResponse{},
		s.localStorage.WriteFile(req.Filename, bytes.NewReader(req.Payload))
}

// List implements the gRPC service.
func (s *Service) List(
	ctx context.Context, req *blobspb.GlobRequest,
) (*blobspb.GlobResponse, error) {
	matches, err := s.localStorage.List(req.Pattern)
	return &blobspb.GlobResponse{Files: matches}, err
}

// Delete implements the gRPC service.
func (s *Service) Delete(
	ctx context.Context, req *blobspb.DeleteRequest,
) (*blobspb.DeleteResponse, error) {
	return &blobspb.DeleteResponse{}, s.localStorage.Delete(req.Filename)
}

// Stat implements the gRPC service.
func (s *Service) Stat(ctx context.Context, req *blobspb.StatRequest) (*blobspb.BlobStat, error) {
	return s.localStorage.Stat(req.Filename)
}
