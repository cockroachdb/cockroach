// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nodelocal

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func parseNodelocalURL(
	_ cloud.ExternalStorageURIContext, uri *url.URL,
) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	if uri.Host == "" {
		return conf, errors.Errorf(
			"host component of nodelocal URI must be a node ID ("+
				"use 'self' to specify each node should access its own local filesystem): %s",
			uri.String(),
		)
	} else if uri.Host == "self" {
		uri.Host = "0"
	}

	nodeID, err := strconv.Atoi(uri.Host)
	if err != nil {
		return conf, errors.Errorf("host component of nodelocal URI must be a node ID: %s", uri.String())
	}
	conf.Provider = roachpb.ExternalStorageProvider_nodelocal
	conf.LocalFile.Path = uri.Path
	conf.LocalFile.NodeID = roachpb.NodeID(nodeID)
	return conf, nil
}

type localFileStorage struct {
	cfg        roachpb.ExternalStorage_LocalFilePath // contains un-prefixed filepath -- DO NOT use for I/O ops.
	ioConf     base.ExternalIODirConfig              // server configurations for the ExternalStorage
	base       string                                // relative filepath prefixed with externalIODir, for I/O ops on this node.
	blobClient blobs.BlobClient                      // inter-node file sharing service
	settings   *cluster.Settings                     // cluster settings for the ExternalStorage
}

var _ cloud.ExternalStorage = &localFileStorage{}

// MakeLocalStorageURI converts a local path (should always be relative) to a
// valid nodelocal URI.
func MakeLocalStorageURI(path string) string {
	return fmt.Sprintf("nodelocal://0/%s", path)
}

// TestingMakeLocalStorage is used by tests.
func TestingMakeLocalStorage(
	ctx context.Context,
	cfg roachpb.ExternalStorage_LocalFilePath,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	ioConf base.ExternalIODirConfig,
) (cloud.ExternalStorage, error) {
	args := cloud.ExternalStorageContext{IOConf: ioConf, BlobClientFactory: blobClientFactory, Settings: settings}
	return makeLocalStorage(ctx, args, roachpb.ExternalStorage{LocalFile: cfg})
}

func makeLocalStorage(
	ctx context.Context, args cloud.ExternalStorageContext, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.nodelocal")
	if args.BlobClientFactory == nil {
		return nil, errors.New("nodelocal storage is not available")
	}
	cfg := dest.LocalFile
	if cfg.Path == "" {
		return nil, errors.Errorf("local storage requested but path not provided")
	}
	client, err := args.BlobClientFactory(ctx, cfg.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create blob client")
	}
	return &localFileStorage{base: cfg.Path, cfg: cfg, ioConf: args.IOConf, blobClient: client,
		settings: args.Settings}, nil
}

func (l *localFileStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:  roachpb.ExternalStorageProvider_nodelocal,
		LocalFile: l.cfg,
	}
}

func (l *localFileStorage) ExternalIOConf() base.ExternalIODirConfig {
	return l.ioConf
}

func (l *localFileStorage) Settings() *cluster.Settings {
	return l.settings
}

func joinRelativePath(filePath string, file string) string {
	// Joining "." to make this a relative path.
	// This ensures path.Clean does not simplify in unexpected ways.
	return path.Join(".", filePath, file)
}

func (l *localFileStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	return l.blobClient.Writer(ctx, joinRelativePath(l.base, basename))
}

// ReadFile is shorthand for ReadFileAt with offset 0.
func (l *localFileStorage) ReadFile(
	ctx context.Context, basename string,
) (ioctx.ReadCloserCtx, error) {
	body, _, err := l.ReadFileAt(ctx, basename, 0)
	return body, err
}

func (l *localFileStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	reader, size, err := l.blobClient.ReadFile(ctx, joinRelativePath(l.base, basename), offset)
	if err != nil {
		// The format of the error returned by the above ReadFile call differs based
		// on whether we are reading from a local or remote nodelocal store.
		// The local store returns a golang native ErrNotFound, whereas the remote
		// store returns a gRPC native NotFound error.
		if oserror.IsNotExist(err) || status.Code(err) == codes.NotFound {
			// nolint:errwrap
			return nil, 0, errors.WithMessagef(
				errors.Wrap(cloud.ErrFileDoesNotExist, "nodelocal storage file does not exist"),
				"%s",
				err.Error(),
			)
		}
		return nil, 0, err
	}
	return reader, size, nil
}

func (l *localFileStorage) List(
	ctx context.Context, prefix, delim string, fn cloud.ListingFn,
) error {
	dest := cloud.JoinPathPreservingTrailingSlash(l.base, prefix)

	res, err := l.blobClient.List(ctx, dest)
	if err != nil {
		return errors.Wrap(err, "unable to match pattern provided")
	}

	// Sort results so that we can group as we go.
	sort.Strings(res)
	var prevPrefix string
	for _, f := range res {
		f = strings.TrimPrefix(f, dest)
		if delim != "" {
			if i := strings.Index(f, delim); i >= 0 {
				f = f[:i+len(delim)]
			}
			if f == prevPrefix {
				continue
			}
			prevPrefix = f
		}
		if err := fn(f); err != nil {
			return err
		}
	}
	return nil
}

func (l *localFileStorage) Delete(ctx context.Context, basename string) error {
	return l.blobClient.Delete(ctx, joinRelativePath(l.base, basename))
}

func (l *localFileStorage) Size(ctx context.Context, basename string) (int64, error) {
	stat, err := l.blobClient.Stat(ctx, joinRelativePath(l.base, basename))
	if err != nil {
		return 0, err
	}
	return stat.Filesize, nil
}

func (*localFileStorage) Close() error {
	return nil
}

func init() {
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_nodelocal,
		parseNodelocalURL, makeLocalStorage, cloud.RedactedParams(), "nodelocal")
}
