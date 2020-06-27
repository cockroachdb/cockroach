// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudimpl

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localFileStorage struct {
	cfg        roachpb.ExternalStorage_LocalFilePath // contains un-prefixed filepath -- DO NOT use for I/O ops.
	base       string                                // relative filepath prefixed with externalIODir, for I/O ops on this node.
	blobClient blobs.BlobClient                      // inter-node file sharing service
}

var _ cloud.ExternalStorage = &localFileStorage{}

// MakeLocalStorageURI converts a local path (should always be relative) to a
// valid nodelocal URI.
func MakeLocalStorageURI(path string) string {
	return fmt.Sprintf("nodelocal://0/%s", path)
}

func makeNodeLocalURIWithNodeID(nodeID roachpb.NodeID, path string) string {
	path = strings.TrimPrefix(path, "/")
	return fmt.Sprintf("nodelocal://%d/%s", nodeID, path)
}

func makeLocalStorage(
	ctx context.Context,
	cfg roachpb.ExternalStorage_LocalFilePath,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (cloud.ExternalStorage, error) {
	if cfg.Path == "" {
		return nil, errors.Errorf("Local storage requested but path not provided")
	}
	client, err := blobClientFactory(ctx, cfg.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create blob client")
	}
	return &localFileStorage{base: cfg.Path, cfg: cfg, blobClient: client}, nil
}

func (l *localFileStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:  roachpb.ExternalStorageProvider_LocalFile,
		LocalFile: l.cfg,
	}
}

func joinRelativePath(filePath string, file string) string {
	// Joining "." to make this a relative path.
	// This ensures path.Clean does not simplify in unexpected ways.
	return path.Join(".", filePath, file)
}

func (l *localFileStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	return l.blobClient.WriteFile(ctx, joinRelativePath(l.base, basename), content)
}

func (l *localFileStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	var err error
	var reader io.ReadCloser
	if reader, err = l.blobClient.ReadFile(ctx, joinRelativePath(l.base, basename)); err != nil {
		// The format of the error returned by the above ReadFile call differs based
		// on whether we are reading from a local or remote nodelocal store.
		// The local store returns a golang native ErrNotFound, whereas the remote
		// store returns a gRPC native NotFound error.
		if os.IsNotExist(err) || status.Code(err) == codes.NotFound {
			return nil, errors.Wrapf(ErrFileDoesNotExist, "nodelocal storage file does not exist: %s", err.Error())
		}
		return nil, err
	}
	return reader, nil
}

func (l *localFileStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {

	pattern := l.base
	if patternSuffix != "" {
		if containsGlob(l.base) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = joinRelativePath(pattern, patternSuffix)
	}

	var fileList []string
	matches, err := l.blobClient.List(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}

	for _, fileName := range matches {
		if patternSuffix != "" {
			if !strings.HasPrefix(fileName, l.base) {
				// TODO(dt): return a nice rel-path instead of erroring out.
				return nil, errors.Errorf("pattern matched file outside of base path %q", l.base)
			}
			fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(fileName, l.base), "/"))
		} else {
			fileList = append(fileList, makeNodeLocalURIWithNodeID(l.cfg.NodeID, fileName))
		}
	}

	return fileList, nil
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
