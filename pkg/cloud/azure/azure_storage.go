// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

const (
	// AzureAccountNameParam is the query parameter for account_name in an azure URI.
	AzureAccountNameParam = "AZURE_ACCOUNT_NAME"
	// AzureAccountKeyParam is the query parameter for account_key in an azure URI.
	AzureAccountKeyParam = "AZURE_ACCOUNT_KEY"
)

func parseAzureURL(
	_ cloud.ExternalStorageURIContext, uri *url.URL,
) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	conf.Provider = roachpb.ExternalStorageProvider_azure
	conf.AzureConfig = &roachpb.ExternalStorage_Azure{
		Container:   uri.Host,
		Prefix:      uri.Path,
		AccountName: uri.Query().Get(AzureAccountNameParam),
		AccountKey:  uri.Query().Get(AzureAccountKeyParam),
	}
	if conf.AzureConfig.AccountName == "" {
		return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountNameParam)
	}
	if conf.AzureConfig.AccountKey == "" {
		return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountKeyParam)
	}
	conf.AzureConfig.Prefix = strings.TrimLeft(conf.AzureConfig.Prefix, "/")
	return conf, nil
}

type azureStorage struct {
	conf      *roachpb.ExternalStorage_Azure
	ioConf    base.ExternalIODirConfig
	container azblob.ContainerURL
	prefix    string
	settings  *cluster.Settings
}

var _ cloud.ExternalStorage = &azureStorage{}

func makeAzureStorage(
	_ context.Context, args cloud.ExternalStorageContext, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.azure")
	conf := dest.AzureConfig
	if conf == nil {
		return nil, errors.Errorf("azure upload requested but info missing")
	}
	credential, err := azblob.NewSharedKeyCredential(conf.AccountName, conf.AccountKey)
	if err != nil {
		return nil, errors.Wrap(err, "azure credential")
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", conf.AccountName))
	if err != nil {
		return nil, errors.Wrap(err, "azure: account name is not valid")
	}
	serviceURL := azblob.NewServiceURL(*u, p)
	return &azureStorage{
		conf:      conf,
		ioConf:    args.IOConf,
		container: serviceURL.NewContainerURL(conf.Container),
		prefix:    conf.Prefix,
		settings:  args.Settings,
	}, nil
}

func (s *azureStorage) getBlob(basename string) azblob.BlockBlobURL {
	name := path.Join(s.prefix, basename)
	return s.container.NewBlockBlobURL(name)
}

func (s *azureStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:    roachpb.ExternalStorageProvider_azure,
		AzureConfig: s.conf,
	}
}

func (s *azureStorage) ExternalIOConf() base.ExternalIODirConfig {
	return s.ioConf
}

func (s *azureStorage) Settings() *cluster.Settings {
	return s.settings
}

func (s *azureStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	ctx, sp := tracing.ChildSpan(ctx, "azure.Writer")
	sp.RecordStructured(&types.StringValue{Value: fmt.Sprintf("azure.Writer: %s",
		path.Join(s.prefix, basename))})
	blob := s.getBlob(basename)
	return cloud.BackgroundPipe(ctx, func(ctx context.Context, r io.Reader) error {
		defer sp.Finish()
		_, err := azblob.UploadStreamToBlockBlob(
			ctx, r, blob, azblob.UploadStreamToBlockBlobOptions{
				BufferSize: 4 << 20,
			},
		)
		return err
	}), nil
}

// ReadFile is shorthand for ReadFileAt with offset 0.
func (s *azureStorage) ReadFile(ctx context.Context, basename string) (ioctx.ReadCloserCtx, error) {
	reader, _, err := s.ReadFileAt(ctx, basename, 0)
	return reader, err
}

func (s *azureStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	ctx, sp := tracing.ChildSpan(ctx, "azure.ReadFileAt")
	defer sp.Finish()
	sp.RecordStructured(&types.StringValue{Value: fmt.Sprintf("azure.ReadFileAt: %s",
		path.Join(s.prefix, basename))})

	// https://github.com/cockroachdb/cockroach/issues/23859
	blob := s.getBlob(basename)
	get, err := blob.Download(ctx, offset, azblob.CountToEnd, azblob.BlobAccessConditions{},
		false /* rangeGetContentMD5 */, azblob.ClientProvidedKeyOptions{},
	)
	if err != nil {
		if azerr := (azblob.StorageError)(nil); errors.As(err, &azerr) {
			switch azerr.ServiceCode() {
			// TODO(adityamaru): Investigate whether both these conditions are required.
			case azblob.ServiceCodeBlobNotFound, azblob.ServiceCodeResourceNotFound:
				// nolint:errwrap
				return nil, 0, errors.Wrapf(
					errors.Wrap(cloud.ErrFileDoesNotExist, "azure blob does not exist"),
					"%v",
					err.Error(),
				)
			}
		}
		return nil, 0, errors.Wrap(err, "failed to create azure reader")
	}
	var size int64
	if offset == 0 {
		size = get.ContentLength()
	} else {
		size, err = cloud.CheckHTTPContentRangeHeader(get.ContentRange(), offset)
		if err != nil {
			return nil, 0, err
		}
	}
	reader := get.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3})

	return ioctx.ReadCloserAdapter(reader), size, nil
}

func (s *azureStorage) List(ctx context.Context, prefix, delim string, fn cloud.ListingFn) error {
	ctx, sp := tracing.ChildSpan(ctx, "azure.List")
	defer sp.Finish()

	dest := cloud.JoinPathPreservingTrailingSlash(s.prefix, prefix)
	sp.RecordStructured(&types.StringValue{Value: fmt.Sprintf("azure.List: %s", dest)})

	var marker azblob.Marker
	for marker.NotDone() {
		response, err := s.container.ListBlobsHierarchySegment(
			ctx, marker, delim, azblob.ListBlobsSegmentOptions{Prefix: dest},
		)
		if err != nil {
			return errors.Wrap(err, "unable to list files for specified blob")
		}
		for _, blob := range response.Segment.BlobPrefixes {
			if err := fn(strings.TrimPrefix(blob.Name, dest)); err != nil {
				return err
			}
		}
		for _, blob := range response.Segment.BlobItems {
			if err := fn(strings.TrimPrefix(blob.Name, dest)); err != nil {
				return err
			}
		}
		marker = response.NextMarker
	}
	return nil
}

func (s *azureStorage) Delete(ctx context.Context, basename string) error {
	err := contextutil.RunWithTimeout(ctx, "delete azure file", cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			return err
		})
	return errors.Wrap(err, "delete file")
}

func (s *azureStorage) Size(ctx context.Context, basename string) (int64, error) {
	var props *azblob.BlobGetPropertiesResponse
	err := contextutil.RunWithTimeout(ctx, "size azure file", cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			var err error
			props, err = blob.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
			return err
		})
	if err != nil {
		return 0, errors.Wrap(err, "get file properties")
	}
	return props.ContentLength(), nil
}

// Close is part of the cloud.ExternalStorage interface.
func (s *azureStorage) Close() error {
	return nil
}

func init() {
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_azure,
		parseAzureURL, makeAzureStorage, cloud.RedactedParams(AzureAccountKeyParam), "azure")
}
