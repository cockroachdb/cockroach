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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Service handles interactions with the inter-node blob sharing service.
type Service struct {
	self   roachpb.NodeID
	base   string
	dialer *nodedialer.Dialer
}

var _ roachpb.BlobServer = &Service{}

// TODO(georgiah): we will need to support Delete() and Size()
// NewBlobService instantiates a blob service server.
func NewBlobService(dialer *nodedialer.Dialer, self roachpb.NodeID, basePath string) *Service {
	return &Service{self: self, dialer: dialer, base: basePath}
}

// WriteFileLocally is a helper function
func (s *Service) WriteFileLocally(filename string, content io.ReadSeeker) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return errors.Wrap(err, "creating local external storage path")
	}
	tmpP := filename + `.tmp`
	f, err := os.Create(tmpP)
	if err != nil {
		return errors.Wrapf(err, "creating local external tmp file %q", tmpP)
	}
	defer func() {
		f.Close()
		if err == nil {
			err = errors.Wrapf(os.Rename(tmpP, filename), "renaming to local export file %q", filename)
		}
	}()
	_, err = io.Copy(f, content)
	if err != nil {
		return errors.Wrapf(err, "writing to local external tmp file %q", tmpP)
	}
	if err := f.Sync(); err != nil {
		return errors.Wrapf(err, "syncing to local external tmp file %q", tmpP)
	}
	return nil
}

func ListLocalFiles(dirName string, externalIODir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(externalIODir, dirName))
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}
	return matches, nil
}

// Get implements the gRPC service.
func (s *Service) Get(
	ctx context.Context, req *roachpb.GetBlobRequest,
) (*roachpb.GetBlobResponse, error) {
	localFile := filepath.Join(s.base, req.Filename)
	payload, err := ioutil.ReadFile(localFile)
	return &roachpb.GetBlobResponse{Payload: payload}, err
}

// Put implements the gRPC service.
func (s *Service) Put(
	ctx context.Context, req *roachpb.PutBlobRequest,
) (*roachpb.PutBlobResponse, error) {
	localFile := filepath.Join(s.base, req.Filename)
	return &roachpb.PutBlobResponse{}, s.WriteFileLocally(localFile, bytes.NewReader(req.Payload))
}

// List implements the gRPC service.
func (s *Service) List(
	ctx context.Context, req *roachpb.ListBlobRequest,
) (*roachpb.ListBlobResponse, error) {
	matches, err := ListLocalFiles(req.Dir, s.base)
	return &roachpb.ListBlobResponse{Files: matches}, err
}

func (s *Service) DialNode(ctx context.Context, nodeID roachpb.NodeID) (*grpc.ClientConn, error) {
	return s.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
}

// Fetch fetches the named payload from the requested node.
// This method should be used before attempting to read a
// file in ExternalStorage's LocalStorage.
func (s *Service) Fetch(ctx context.Context, from roachpb.NodeID, file string) error {
	if from == s.self {
		return nil
	}
	conn, err := s.DialNode(ctx, from)
	if err != nil {
		return errors.Wrap(err, "connecting to node")
	}
	client := roachpb.NewBlobClient(conn)
	resp, err := client.Get(ctx, &roachpb.GetBlobRequest{
		Filename: file,
	})
	if err != nil {
		return errors.Wrap(err, "fetching file")
	}

	err = s.WriteFileLocally(filepath.Join(s.base, file), bytes.NewReader(resp.Payload))
	if err != nil {
		return err
	}
	return nil
}

// Send sends the named payload to the requested node.
// This method will write the file either locally,
// or send it over to another node, based on the nodeID.
func (s *Service) Send(ctx context.Context, to roachpb.NodeID, file string, payload []byte) error {
	if to == s.self {
		return s.WriteFileLocally(filepath.Join(s.base, file), bytes.NewReader(payload))
	}
	conn, err := s.DialNode(ctx, to)
	if err != nil {
		return errors.Wrap(err, "connecting to node")
	}
	client := roachpb.NewBlobClient(conn)
	_, err = client.Put(ctx, &roachpb.PutBlobRequest{
		Filename: file,
		Payload:  payload,
	})
	return err
}

// FetchList lists the corresponding filenames from the requested node.
// The requested node can be the current node.
func (s *Service) FetchList(
	ctx context.Context, from roachpb.NodeID, dir string,
) ([]string, error) {
	if from == s.self {
		return ListLocalFiles(dir, s.base)
	}
	conn, err := s.DialNode(ctx, from)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to node")
	}
	client := roachpb.NewBlobClient(conn)
	resp, err := client.List(ctx, &roachpb.ListBlobRequest{
		Dir: dir,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetching list")
	}
	return resp.Files, nil
}
