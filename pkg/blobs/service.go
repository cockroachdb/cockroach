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
	"context"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
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

// GetStream implements the gRPC service.
func (s *Service) GetStream(req *blobspb.GetRequest, stream blobspb.Blob_GetStreamServer) error {
	content, err := s.localStorage.ReadFile(req.Filename)
	if err != nil {
		return err
	}
	defer content.Close()
	return streamContent(stream, content)
}

// PutStream implements the gRPC service.
func (s *Service) PutStream(stream blobspb.Blob_PutStreamServer) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return errors.New("could not fetch metadata")
	}
	filename := md.Get("filename")
	if len(filename) < 1 || filename[0] == "" {
		return errors.New("no filename in metadata")
	}
	reader := newPutStreamReader(stream)
	defer reader.Close()
	err := s.localStorage.WriteFile(filename[0], reader)
	return err
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
