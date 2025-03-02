// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const scheme = "nodelocal"

func validateLocalFileURI(uri *url.URL) error {
	if uri.Host == "" {
		return errors.Newf(
			"host component of nodelocal URI must be a node ID ("+
				"use 'self' to specify each node should access its own local filesystem): %s",
			uri.String(),
		)
	} else if uri.Host == "self" {
		uri.Host = "0"
	}

	_, err := strconv.Atoi(uri.Host)
	if err != nil {
		return errors.Newf("host component of nodelocal URI must be a node ID: %s", uri.String())
	}

	// TODO(adityamaru): We should be restricting the URI params that nodelocal
	// accepts but there are several tests that use `nodelocal` to test URI params
	// for other ExternalStorage providers. Fix those and then invoke
	// `cloud.ValidateQueryParams` with the allow-list of parameters.
	return nil
}

func makeLocalFileConfig(uri *url.URL) (cloudpb.ExternalStorage_LocalFileConfig, error) {
	localCfg := cloudpb.ExternalStorage_LocalFileConfig{}
	nodeID, err := strconv.Atoi(uri.Host)
	if err != nil {
		return localCfg, errors.Errorf("host component of nodelocal URI must be a node ID: %s", uri.String())
	}
	localCfg.Path = uri.Path
	localCfg.NodeID = roachpb.NodeID(nodeID)

	return localCfg, nil
}

func parseLocalFileURI(
	_ cloud.ExternalStorageURIContext, uri *url.URL,
) (cloudpb.ExternalStorage, error) {
	conf := cloudpb.ExternalStorage{}

	if err := validateLocalFileURI(uri); err != nil {
		return conf, errors.Wrap(err, "invalid `nodelocal` URI")
	}

	conf.Provider = cloudpb.ExternalStorageProvider_nodelocal
	var err error
	conf.LocalFileConfig, err = makeLocalFileConfig(uri)
	return conf, err
}

type localFileStorage struct {
	cfg        cloudpb.ExternalStorage_LocalFileConfig // contains un-prefixed filepath -- DO NOT use for I/O ops.
	ioConf     base.ExternalIODirConfig                // server configurations for the ExternalStorage
	base       string                                  // relative filepath prefixed with externalIODir, for I/O ops on this node.
	blobClient blobs.BlobClient                        // inter-node file sharing service
	settings   *cluster.Settings                       // cluster settings for the ExternalStorage
}

var _ cloud.ExternalStorage = &localFileStorage{}

// LocalRequiresExternalIOAccounting is the return values for
// (*localFileStorage).RequiresExternalIOAccounting. This is exposed for
// testing.
var LocalRequiresExternalIOAccounting = false

// MakeLocalStorageURI converts a local path (should always be relative) to a
// valid nodelocal URI.
func MakeLocalStorageURI(path string) string {
	return fmt.Sprintf("nodelocal://1/%s", path)
}

func makeLocalFileStorage(
	ctx context.Context, args cloud.ExternalStorageContext, dest cloudpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.nodelocal")
	if args.BlobClientFactory == nil {
		return nil, errors.New("nodelocal storage is not available")
	}
	cfg := dest.LocalFileConfig
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

func (l *localFileStorage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{
		Provider:        cloudpb.ExternalStorageProvider_nodelocal,
		LocalFileConfig: l.cfg,
	}
}

func (l *localFileStorage) ExternalIOConf() base.ExternalIODirConfig {
	return l.ioConf
}

func (l *localFileStorage) RequiresExternalIOAccounting() bool {
	return LocalRequiresExternalIOAccounting
}

func (l *localFileStorage) Settings() *cluster.Settings {
	return l.settings
}

func joinRelativePath(filePath string, file string) string {
	// Joining "." to make this a relative path.
	// This ensures path.Clean does not simplify in unexpected ways.
	return path.Join(".", filePath, file)
}

// isNotFoundErr checks if the error indicates a file not found condition,
// handling both local and remote nodelocal store cases.
func isNotFoundErr(err error) bool {
	return oserror.IsNotExist(err) || status.Code(err) == codes.NotFound
}

func (l *localFileStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	return l.blobClient.Writer(ctx, joinRelativePath(l.base, basename))
}

func (l *localFileStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	reader, size, err := l.blobClient.ReadFile(ctx, joinRelativePath(l.base, basename), opts.Offset)
	if err != nil && isNotFoundErr(err) {
		return nil, 0, cloud.WrapErrFileDoesNotExist(err, "nodelocal storage file does not exist")
	}
	if err != nil {
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
	err := l.blobClient.Delete(ctx, joinRelativePath(l.base, basename))
	if isNotFoundErr(err) {
		return nil
	}
	return err
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
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_nodelocal,
		cloud.RegisteredProvider{
			ConstructFn:    makeLocalFileStorage,
			ParseFn:        parseLocalFileURI,
			RedactedParams: cloud.RedactedParams(),
			Schemes:        []string{scheme},
		})
}
