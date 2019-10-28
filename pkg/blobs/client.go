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

	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/pkg/errors"
)

// BlobClient provides an interface for file access on all nodes' local storage.
// Given the nodeID of the node on which the operation should occur, the a blob
// client should be able to find the correct node and call its blob service API.
type BlobClient interface {
	// ReadFile fetches the named payload from the requested node,
	// and stores it in memory. It then returns an io.ReadCloser to
	// read the contents.
	// TODO(georgiah): this currently sends the entire file over
	// 	over the wire. Still need to implement streaming.
	ReadFile(ctx context.Context, from roachpb.NodeID, file string) (io.ReadCloser, error)

	// WriteFile sends the named payload to the requested node.
	// This method will read entire content of file and send
	// it over to another node, based on the nodeID.
	// TODO(georgiah): this currently sends the entire file over
	// 	over the wire. Still need to implement streaming.
	WriteFile(ctx context.Context, to roachpb.NodeID, file string, content io.ReadSeeker) error

	// List lists the corresponding filenames from the requested node.
	// The requested node can be the current node.
	List(ctx context.Context, from roachpb.NodeID, pattern string) ([]string, error)

	// Delete deletes the specified file or empty directory from a remote node.
	Delete(ctx context.Context, from roachpb.NodeID, file string) error

	// Stat gets the size (in bytes) of a specified file from a remote node.
	Stat(ctx context.Context, from roachpb.NodeID, file string) (*blobspb.BlobStat, error)
}

var _ BlobClient = &remoteClient{}

// remoteClient uses the node dialer and blob service clients
// to Read or Write bulk files from/to other nodes.
type remoteClient struct {
	dialer *nodedialer.Dialer
}

// newRemoteClient instantiates a remote blob service client.
func newRemoteClient(dialer *nodedialer.Dialer) BlobClient {
	return &remoteClient{dialer: dialer}
}

func (c *remoteClient) getBlobClient(
	ctx context.Context, nodeID roachpb.NodeID,
) (blobspb.BlobClient, error) {
	conn, err := c.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to node")
	}
	return blobspb.NewBlobClient(conn), nil
}

func (c *remoteClient) ReadFile(
	ctx context.Context, from roachpb.NodeID, file string,
) (io.ReadCloser, error) {
	client, err := c.getBlobClient(ctx, from)
	if err != nil {
		return nil, err
	}
	resp, err := client.GetBlob(ctx, &blobspb.GetRequest{
		Filename: file,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetching file")
	}
	return ioutil.NopCloser(bytes.NewReader(resp.Payload)), err
}

func (c *remoteClient) WriteFile(
	ctx context.Context, to roachpb.NodeID, file string, content io.ReadSeeker,
) error {
	payload, err := ioutil.ReadAll(content)
	if err != nil {
		return errors.Wrap(err, "reading file contents")
	}

	blobClient, err := c.getBlobClient(ctx, to)
	if err != nil {
		return err
	}

	b, err := blobClient.PutBlob(ctx, &blobspb.PutRequest{
		Filename: file,
		Payload:  payload,
	})
	b.Size()
	return err
}

func (c *remoteClient) List(
	ctx context.Context, from roachpb.NodeID, pattern string,
) ([]string, error) {
	blobClient, err := c.getBlobClient(ctx, from)
	if err != nil {
		return nil, err
	}

	resp, err := blobClient.List(ctx, &blobspb.GlobRequest{
		Pattern: pattern,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetching list")
	}
	return resp.Files, nil
}

func (c *remoteClient) Delete(ctx context.Context, from roachpb.NodeID, file string) error {
	blobClient, err := c.getBlobClient(ctx, from)
	if err != nil {
		return err
	}

	_, err = blobClient.Delete(ctx, &blobspb.DeleteRequest{
		Filename: file,
	})
	return err
}

func (c *remoteClient) Stat(
	ctx context.Context, from roachpb.NodeID, file string,
) (*blobspb.BlobStat, error) {
	blobClient, err := c.getBlobClient(ctx, from)
	if err != nil {
		return nil, err
	}

	resp, err := blobClient.Stat(ctx, &blobspb.StatRequest{
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
	localStorage *localStorage
}

// newLocalClient instantiates a local blob service client.
func newLocalClient(externalIODir string) (BlobClient, error) {
	storage, err := newLocalStorage(externalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating local client")
	}
	return &localClient{localStorage: storage}, nil
}

func (c *localClient) ReadFile(
	ctx context.Context, _ roachpb.NodeID, file string,
) (io.ReadCloser, error) {
	return c.localStorage.ReadFile(file)
}

func (c *localClient) WriteFile(
	ctx context.Context, _ roachpb.NodeID, file string, content io.ReadSeeker,
) error {
	return c.localStorage.WriteFile(file, content)
}

func (c *localClient) List(
	ctx context.Context, _ roachpb.NodeID, pattern string,
) ([]string, error) {
	return c.localStorage.List(pattern)
}

func (c *localClient) Delete(ctx context.Context, _ roachpb.NodeID, file string) error {
	return c.localStorage.Delete(file)
}

func (c *localClient) Stat(
	ctx context.Context, _ roachpb.NodeID, file string,
) (*blobspb.BlobStat, error) {
	return c.localStorage.Stat(file)
}

var _ BlobClient = &wrapperClient{}

// wrapperClient decides between using localClient
// or remoteClient, based on the node ID.
type wrapperClient struct {
	self         roachpb.NodeID
	remoteClient BlobClient
	localClient  BlobClient
}

// NewBlobClient instantiates a wrapper blob service client.
func NewBlobClient(
	self roachpb.NodeID, dialer *nodedialer.Dialer, localExternalIODir string,
) (BlobClient, error) {
	localClient, err := newLocalClient(localExternalIODir)
	if err != nil {
		return nil, err
	}
	return &wrapperClient{
		self:         self,
		remoteClient: newRemoteClient(dialer),
		localClient:  localClient,
	}, nil
}

func (c *wrapperClient) getClient(nodeID roachpb.NodeID) BlobClient {
	if nodeID == c.self {
		return c.localClient
	}
	return c.remoteClient
}

func (c *wrapperClient) ReadFile(
	ctx context.Context, from roachpb.NodeID, file string,
) (io.ReadCloser, error) {
	client := c.getClient(from)
	return client.ReadFile(ctx, from, file)
}

func (c *wrapperClient) WriteFile(
	ctx context.Context, to roachpb.NodeID, file string, content io.ReadSeeker,
) error {
	client := c.getClient(to)
	return client.WriteFile(ctx, to, file, content)
}

func (c *wrapperClient) List(
	ctx context.Context, from roachpb.NodeID, pattern string,
) ([]string, error) {
	client := c.getClient(from)
	return client.List(ctx, from, pattern)
}

func (c *wrapperClient) Delete(ctx context.Context, from roachpb.NodeID, file string) error {
	client := c.getClient(from)
	return client.Delete(ctx, from, file)
}

func (c *wrapperClient) Stat(
	ctx context.Context, from roachpb.NodeID, file string,
) (*blobspb.BlobStat, error) {
	client := c.getClient(from)
	return client.Stat(ctx, from, file)
}
