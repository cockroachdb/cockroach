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
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

// BlobClient provides an interface for file access on all nodes' local storage.
// Given the nodeID of the node on which the operation should occur, the a blob
// client should be able to find the correct node and call its blob service API.
type BlobClient interface {
	// ReadFile fetches the named payload from the requested node,
	// and stores it in memory. It then returns an io.ReadCloser to
	// read the contents.
	ReadFile(ctx context.Context, file string) (io.ReadCloser, error)

	// WriteFile sends the named payload to the requested node.
	// This method will read entire content of file and send
	// it over to another node, based on the nodeID.
	WriteFile(ctx context.Context, file string, content io.ReadSeeker) error

	// List lists the corresponding filenames from the requested node.
	// The requested node can be the current node.
	List(ctx context.Context, pattern string) ([]string, error)

	// Delete deletes the specified file or empty directory from a remote node.
	Delete(ctx context.Context, file string) error

	// Stat gets the size (in bytes) of a specified file from a remote node.
	Stat(ctx context.Context, file string) (*blobspb.BlobStat, error)
}

var _ BlobClient = &remoteClient{}

// remoteClient uses the node dialer and blob service clients
// to Read or Write bulk files from/to other nodes.
type remoteClient struct {
	blobClient blobspb.BlobClient
}

// newRemoteClient instantiates a remote blob service client.
func newRemoteClient(blobClient blobspb.BlobClient) BlobClient {
	return &remoteClient{blobClient: blobClient}
}

func (c *remoteClient) ReadFile(ctx context.Context, file string) (io.ReadCloser, error) {
	// Check that file exists before reading from it
	_, err := c.Stat(ctx, file)
	if err != nil {
		return nil, err
	}
	stream, err := c.blobClient.GetStream(ctx, &blobspb.GetRequest{
		Filename: file,
	})
	return newGetStreamReader(stream), errors.Wrap(err, "fetching file")
}

func (c *remoteClient) WriteFile(
	ctx context.Context, file string, content io.ReadSeeker,
) (err error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "filename", file)
	stream, err := c.blobClient.PutStream(ctx)
	if err != nil {
		return
	}
	defer func() {
		_, closeErr := stream.CloseAndRecv()
		if err == nil {
			err = closeErr
		}
	}()
	err = streamContent(stream, content)
	return
}

func (c *remoteClient) List(ctx context.Context, pattern string) ([]string, error) {
	resp, err := c.blobClient.List(ctx, &blobspb.GlobRequest{
		Pattern: pattern,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetching list")
	}
	return resp.Files, nil
}

func (c *remoteClient) Delete(ctx context.Context, file string) error {
	_, err := c.blobClient.Delete(ctx, &blobspb.DeleteRequest{
		Filename: file,
	})
	return err
}

func (c *remoteClient) Stat(ctx context.Context, file string) (*blobspb.BlobStat, error) {
	resp, err := c.blobClient.Stat(ctx, &blobspb.StatRequest{
		Filename: file,
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

var _ BlobClient = &localClient{}

// localClient executes the local blob service's code
// to Read or Write bulk files on the current node.
type localClient struct {
	localStorage *LocalStorage
}

// newLocalClient instantiates a local blob service client.
func newLocalClient(externalIODir string) (BlobClient, error) {
	storage, err := NewLocalStorage(externalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating local client")
	}
	return &localClient{localStorage: storage}, nil
}

func (c *localClient) ReadFile(ctx context.Context, file string) (io.ReadCloser, error) {
	return c.localStorage.ReadFile(file)
}

func (c *localClient) WriteFile(ctx context.Context, file string, content io.ReadSeeker) error {
	return c.localStorage.WriteFile(file, content)
}

func (c *localClient) List(ctx context.Context, pattern string) ([]string, error) {
	return c.localStorage.List(pattern)
}

func (c *localClient) Delete(ctx context.Context, file string) error {
	return c.localStorage.Delete(file)
}

func (c *localClient) Stat(ctx context.Context, file string) (*blobspb.BlobStat, error) {
	return c.localStorage.Stat(file)
}

// BlobClientFactory creates a blob client based on the nodeID we are dialing.
type BlobClientFactory func(ctx context.Context, dialing roachpb.NodeID) (BlobClient, error)

// NewBlobClientFactory returns a BlobClientFactory
func NewBlobClientFactory(
	localNodeID roachpb.NodeID, dialer *nodedialer.Dialer, externalIODir string,
) BlobClientFactory {
	return func(ctx context.Context, dialing roachpb.NodeID) (BlobClient, error) {
		if dialing == 0 || localNodeID == dialing {
			return newLocalClient(externalIODir)
		}
		conn, err := dialer.Dial(ctx, dialing, rpc.DefaultClass)
		if err != nil {
			return nil, errors.Wrapf(err, "connecting to node %d", dialing)
		}
		return newRemoteClient(blobspb.NewBlobClient(conn)), nil
	}
}
