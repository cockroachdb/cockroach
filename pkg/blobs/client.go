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
	"github.com/pkg/errors"
)

// Client uses the node dialer and blob service clients
// to Send or Fetch bulk files from/to other nodes.
type Client struct {
	self        roachpb.NodeID
	dialer      *nodedialer.Dialer
	localClient blobspb.BlobClient
}

// NewBlobClient instantiates a blob service client.
func NewBlobClient(dialer *nodedialer.Dialer, self roachpb.NodeID) *Client {
	return &Client{dialer: dialer, self: self}
}

func (c *Client) getBlobClient(
	ctx context.Context, nodeID roachpb.NodeID,
) (blobspb.BlobClient, error) {
	var client blobspb.BlobClient
	if nodeID == c.self {
		if c.localClient != nil {
			return c.localClient, nil
		}
		defer func() {
			c.localClient = client
		}()
	}
	conn, err := c.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to node")
	}
	client = blobspb.NewBlobClient(conn)
	return client, nil
}

// Fetch fetches the named payload from the requested node,
// and stores it locally, in the same location.
// This method should be used before attempting to read a
// file in ExternalStorage's LocalStorage.
func (c *Client) Fetch(ctx context.Context, from roachpb.NodeID, file string) error {
	if from == c.self {
		// File is should already be on current node
		return nil
	}
	client, err := c.getBlobClient(ctx, from)
	if err != nil {
		return err
	}
	resp, err := client.Get(ctx, &blobspb.GetBlobRequest{
		Filename: file,
	})
	if err != nil {
		return errors.Wrap(err, "fetching file")
	}

	// Write it to same location locally
	localClient, err := c.getBlobClient(ctx, c.self)
	if err != nil {
		return errors.Wrap(err, "saving file")
	}
	_, err = localClient.Put(ctx, &blobspb.PutBlobRequest{
		Filename: file,
		Payload:  resp.Payload,
	})
	return err
}

// Send sends the named payload to the requested node.
// This method will write the file either locally,
// or send it over to another node, based on the nodeID.
// TODO(georgiah): this currently only supports files smaller
//  than 1 KB in size, will implement streaming or pagination next
func (c *Client) Send(
	ctx context.Context, to roachpb.NodeID, file string, content io.ReadSeeker,
) error {
	size := 1000 // 1 KB
	payload := make([]byte, size)
	n, err := content.Read(payload)
	if err != nil {
		return err
	}
	if n == size {
		return errors.New("currently can only support files smaller than 1 KB")
	}

	blobClient, err := c.getBlobClient(ctx, to)
	if err != nil {
		return err
	}

	_, err = blobClient.Put(ctx, &blobspb.PutBlobRequest{
		Filename: file,
		Payload:  payload[:n],
	})
	return err
}

// FetchList lists the corresponding filenames from the requested node.
// The requested node can be the current node.
func (c *Client) FetchList(
	ctx context.Context, from roachpb.NodeID, pattern string,
) ([]string, error) {
	blobClient, err := c.getBlobClient(ctx, from)
	if err != nil {
		return nil, err
	}

	resp, err := blobClient.List(ctx, &blobspb.ListBlobRequest{
		Dir: pattern,
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

// FetchSize gets the size (in bytes) of a specified file from a remote node.
func (c *Client) FetchSize(ctx context.Context, from roachpb.NodeID, file string) (int64, error) {
	blobClient, err := c.getBlobClient(ctx, from)
	if err != nil {
		return 0, err
	}

	resp, err := blobClient.Size(ctx, &blobspb.GetBlobSizeRequest{
		Filename: file,
	})
	if err != nil {
		return 0, err
	}
	return resp.Filesize, nil
}
