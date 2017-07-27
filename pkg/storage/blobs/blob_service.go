// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blobs

import (
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/net/context"

	"path/filepath"

	"io/ioutil"

	"strconv"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/pkg/errors"
)

// Service handles interactions with the internode blob sharing service.
type Service struct {
	self   roachpb.NodeID
	base   string
	dialer gossip.GRPCDialer
}

var _ roachpb.BlobServer = &Service{}

// NewBlobService instantiates a blob service server.
func NewBlobService(dialer gossip.GRPCDialer, self roachpb.NodeID, basePath string) *Service {
	return &Service{self: self, dialer: dialer, base: basePath}
}

// Get implements the gRPC service.
func (s *Service) Get(
	ctx context.Context, req *roachpb.GetBlobRequest,
) (*roachpb.GetBlobResponse, error) {
	localFile := filepath.Join(s.base, req.File)
	payload, err := ioutil.ReadFile(localFile)
	return &roachpb.GetBlobResponse{Payload: payload}, err
}

// Put implements the gRPC service.
func (s *Service) Put(
	ctx context.Context, req *roachpb.PutBlobRequest,
) (*roachpb.PutBlobResponse, error) {
	localFile := filepath.Join(s.base, req.File)
	return &roachpb.PutBlobResponse{}, ioutil.WriteFile(localFile, req.Payload, 0600)
}

// StoreLocally adds a file to the local blob storage, and returns a name that
// incldudes the NodeID such that `Fetch` on another node can find it.
func (s *Service) StoreLocally(blob io.Reader, basename string) (qualifiedName string, _ error) {
	f, err := os.Create(filepath.Join(s.base, basename))
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, blob); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", basename, s.self), nil
}

// ParseAndFetch fetches the named payload from the node specified in its name.
func (s *Service) ParseAndFetch(ctx context.Context, file string) ([]byte, error) {
	pos := strings.LastIndex(file, ":")
	if pos < 0 {
		return nil, errors.Errorf("invalid filename: %q", file)
	}
	i, err := strconv.Atoi(file[pos+1:])
	if err != nil {
		return nil, errors.Errorf("invalid node id in filename: %q", file[pos:])
	}
	return s.Fetch(ctx, roachpb.NodeID(i), file[:pos])
}

// Fetch fetches the named payload from the requested node.
func (s *Service) Fetch(ctx context.Context, from roachpb.NodeID, file string) ([]byte, error) {
	conn, err := s.dialer.Dial(from)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to node")
	}
	client := roachpb.NewBlobClient(conn)
	resp, err := client.Get(ctx, &roachpb.GetBlobRequest{
		File: file,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetching blob")
	}
	return resp.Payload, nil
}

// Send sends the named payload to the requested node.
func (s *Service) Send(ctx context.Context, to roachpb.NodeID, file string, payload []byte) error {
	conn, err := s.dialer.Dial(to)
	if err != nil {
		return errors.Wrap(err, "connecting to node")
	}
	client := roachpb.NewBlobClient(conn)
	_, err = client.Put(ctx, &roachpb.PutBlobRequest{
		File:    file,
		Payload: payload,
	})
	return err
}
