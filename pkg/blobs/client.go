// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobs

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs/blobspb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
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
	ReadFile(ctx context.Context, file string, offset int64) (ioctx.ReadCloserCtx, int64, error)

	// Writer opens the named payload on the requested node for writing.
	Writer(ctx context.Context, file string) (io.WriteCloser, error)

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
	blobClient blobspb.RPCBlobClient
	settings   *cluster.Settings
}

// newRemoteClient instantiates a remote blob service client.
func newRemoteClient(blobClient blobspb.RPCBlobClient, settings *cluster.Settings) BlobClient {
	return &remoteClient{blobClient: blobClient, settings: settings}
}

func (c *remoteClient) ReadFile(
	ctx context.Context, file string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	// Check that file exists before reading from it and get size to return.
	st, err := c.Stat(ctx, file)
	if err != nil {
		return nil, 0, err
	}

	// Use flow-controlled streaming if >= 26.2
	if c.settings != nil &&
		c.settings.Version.IsActive(ctx, clusterversion.V26_2) {
		return c.readFileWithFlowControl(ctx, file, offset, st.Filesize)
	}

	// Fall back to legacy streaming.
	return c.readFileLegacy(ctx, file, offset, st.Filesize)
}

// readFileLegacy uses the original GetStream RPC without flow control.
func (c *remoteClient) readFileLegacy(
	ctx context.Context, file string, offset int64, size int64,
) (ioctx.ReadCloserCtx, int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := c.blobClient.GetStream(ctx, &blobspb.GetRequest{
		Filename: file,
		Offset:   offset,
	})
	if err != nil {
		cancel()
		return nil, 0, errors.Wrap(err, "fetching file")
	}
	return newGetStreamReader(stream, cancel), size, nil
}

// readFileWithFlowControl uses the GetStreamFlowControlled RPC with explicit
// acknowledgments to control the pace of data transfer.
func (c *remoteClient) readFileWithFlowControl(
	ctx context.Context, file string, offset int64, size int64,
) (ioctx.ReadCloserCtx, int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := c.blobClient.GetStreamFlowControlled(ctx)
	if err != nil {
		cancel()
		return nil, 0, errors.Wrap(err, "opening flow-controlled stream")
	}

	// Send initial request.
	req := &blobspb.FlowControlledClientMessage{
		Msg: &blobspb.FlowControlledClientMessage_Request{
			Request: &blobspb.FlowControlledGetRequest{
				Filename:          file,
				Offset:            offset,
				FlowControlWindow: int32(FlowControlWindow.Get(&c.settings.SV)),
			},
		},
	}
	if err := stream.Send(req); err != nil {
		cancel()
		return nil, 0, errors.Wrap(err, "sending initial request")
	}

	return newFlowControlledStreamReader(stream, cancel), size, nil
}

type streamWriter struct {
	s   blobspb.RPCBlob_PutStreamClient
	buf blobspb.StreamChunk
}

func (w *streamWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		l := copy(w.buf.Payload[:cap(w.buf.Payload)], p)
		w.buf.Payload = w.buf.Payload[:l]
		p = p[l:]
		if l > 0 {
			if err := w.s.Send(&w.buf); err != nil {
				return n, err
			}
		}
		n += l
	}
	return n, nil
}

func (w *streamWriter) Close() error {
	_, err := w.s.CloseAndRecv()
	return err
}

func (c *remoteClient) Writer(ctx context.Context, file string) (io.WriteCloser, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "filename", file)
	stream, err := c.blobClient.PutStream(ctx)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, ChunkSize)
	return &streamWriter{s: stream, buf: blobspb.StreamChunk{Payload: buf}}, nil
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

// NewLocalClient instantiates a local blob service client.
func NewLocalClient(externalIODir string) (BlobClient, error) {
	storage, err := NewLocalStorage(externalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating local client")
	}
	return &localClient{localStorage: storage}, nil
}

func (c *localClient) ReadFile(
	ctx context.Context, file string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	return c.localStorage.ReadFile(file, offset)
}

func (c *localClient) Writer(ctx context.Context, file string) (io.WriteCloser, error) {
	return c.localStorage.Writer(ctx, file)
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
type BlobClientFactory func(ctx context.Context, dialTarget roachpb.NodeID) (BlobClient, error)

// NewBlobClientFactory returns a BlobClientFactory.
//
// externalIODIr is used to construct a local client when the local
// node is being targetted and the fast path is allowed.
//
// allowLocalFastpath indicates whether the client should create a
// local client (which doesn't go through the network). The fast path
// skips client capability checking so should be used with care.
func NewBlobClientFactory(
	localNodeIDContainer *base.SQLIDContainer,
	dialer *nodedialer.Dialer,
	externalIODir string,
	allowLocalFastpath bool,
	settings *cluster.Settings,
) BlobClientFactory {
	return func(ctx context.Context, dialTarget roachpb.NodeID) (BlobClient, error) {
		localNodeID, ok := localNodeIDContainer.OptionalNodeID()
		if ok && dialTarget == 0 {
			dialTarget = localNodeID
		} else if dialTarget == 0 {
			return nil, errors.New("node ID 0 not supported")
		}

		if localNodeID == dialTarget && allowLocalFastpath {
			return NewLocalClient(externalIODir)
		}
		client, err := blobspb.DialBlobClient(dialer, ctx, dialTarget, rpcbase.DefaultClass)
		if err != nil {
			return nil, errors.Wrapf(err, "connecting to node %d", dialTarget)
		}
		return newRemoteClient(client, settings), nil
	}
}

// NewLocalOnlyBlobClientFactory returns a BlobClientFactory that only
// handles requests for the local node, identified by NodeID = 0.
func NewLocalOnlyBlobClientFactory(externalIODir string) BlobClientFactory {
	return func(ctx context.Context, dialing roachpb.NodeID) (BlobClient, error) {
		if dialing == 0 {
			return NewLocalClient(externalIODir)
		}
		return nil, errors.Errorf("connecting to remote node not supported")
	}
}
