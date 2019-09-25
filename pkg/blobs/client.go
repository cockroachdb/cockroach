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

// Client uses the node dialer and blob service clients
// to Read or Write bulk files from/to other nodes.
type Client struct {
	self   roachpb.NodeID
	dialer *nodedialer.Dialer
}

// NewBlobClient instantiates a blob service client.
func NewBlobClient(dialer *nodedialer.Dialer, self roachpb.NodeID) *Client {
	return &Client{dialer: dialer, self: self}
}

func (c *Client) getBlobClient(
	ctx context.Context, nodeID roachpb.NodeID,
) (blobspb.BlobClient, error) {
	conn, err := c.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to node")
	}
	return blobspb.NewBlobClient(conn), nil
}

// ReadFile fetches the named payload from the requested node,
// and stores it in memory. It then returns a ReadCloser to
// read the contents.
// TODO(georgiah): this currently sends the entire file over
// 	over the wire. Still need to implement streaming.
func (c *Client) ReadFile(
	ctx context.Context, from roachpb.NodeID, file string,
) (io.ReadCloser, error) {
	client, err := c.getBlobClient(ctx, from)
	if err != nil {
		return nil, err
	}
	resp, err := client.GetBlob(ctx, &blobspb.GetBlobRequest{
		Filename: file,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetching file")
	}
	return ioutil.NopCloser(bytes.NewReader(resp.Payload)), err
}

// WriteFile sends the named payload to the requested node.
// This method will read entire content of file and send
// it over to another node, based on the nodeID.
// TODO(georgiah): this currently sends the entire file over
// 	over the wire. Still need to implement streaming.
func (c *Client) WriteFile(
	ctx context.Context, to roachpb.NodeID, file string, content io.ReadSeeker,
) error {
	payload, err := ioutil.ReadAll(content)
	if err != nil {
		return err
	}

	blobClient, err := c.getBlobClient(ctx, to)
	if err != nil {
		return err
	}

	_, err = blobClient.PutBlob(ctx, &blobspb.PutBlobRequest{
		Filename: file,
		Payload:  payload,
	})
	return err
}

// List lists the corresponding filenames from the requested node.
// The requested node can be the current node.
func (c *Client) List(ctx context.Context, from roachpb.NodeID, pattern string) ([]string, error) {
	blobClient, err := c.getBlobClient(ctx, from)
	if err != nil {
		return nil, err
	}

	resp, err := blobClient.List(ctx, &blobspb.ListBlobRequest{
		Pattern: pattern,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetching list")
	}
	return resp.Files, nil
}

// DeleteFrom deletes the specified file or empty directory from a remote node.
func (c *Client) DeleteFrom(ctx context.Context, from roachpb.NodeID, file string) error {
	blobClient, err := c.getBlobClient(ctx, from)
	if err != nil {
		return err
	}

	_, err = blobClient.Delete(ctx, &blobspb.DeleteBlobRequest{
		Filename: file,
	})
	return err
}

// Stat gets the size (in bytes) of a specified file from a remote node.
func (c *Client) Stat(
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

// GetNodeID can be used to check the current node.
func (c *Client) GetNodeID() roachpb.NodeID {
	return c.self
}
