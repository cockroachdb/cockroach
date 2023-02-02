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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/attribute"
)

var maxConcurrentUploadBuffers = settings.RegisterIntSetting(
	settings.TenantWritable,
	"cloudstorage.azure.concurrent_upload_buffers",
	"controls the number of concurrent buffers that will be used by the Azure client when uploading chunks."+
		"Each buffer can buffer up to cloudstorage.write_chunk.size of memory during an upload",
	1,
).WithPublic()

const (
	// AzureAccountNameParam is the query parameter for account_name in an azure URI.
	AzureAccountNameParam = "AZURE_ACCOUNT_NAME"
	// AzureAccountKeyParam is the query parameter for account_key in an azure URI.
	AzureAccountKeyParam = "AZURE_ACCOUNT_KEY"
	// AzureEnvironmentKeyParam is the query parameter for the environment name in an azure URI.
	AzureEnvironmentKeyParam = "AZURE_ENVIRONMENT"

	scheme = "azure-blob"

	deprecatedScheme                   = "azure"
	deprecatedExternalConnectionScheme = "azure-storage"
)

func parseAzureURL(
	_ cloud.ExternalStorageURIContext, uri *url.URL,
) (cloudpb.ExternalStorage, error) {
	azureURL := cloud.ConsumeURL{URL: uri}
	conf := cloudpb.ExternalStorage{}
	conf.Provider = cloudpb.ExternalStorageProvider_azure
	conf.AzureConfig = &cloudpb.ExternalStorage_Azure{
		Container:   uri.Host,
		Prefix:      uri.Path,
		AccountName: azureURL.ConsumeParam(AzureAccountNameParam),
		AccountKey:  azureURL.ConsumeParam(AzureAccountKeyParam),
		Environment: azureURL.ConsumeParam(AzureEnvironmentKeyParam),
	}

	// Validate that all the passed in parameters are supported.
	if unknownParams := azureURL.RemainingQueryParams(); len(unknownParams) > 0 {
		return cloudpb.ExternalStorage{}, errors.Errorf(
			`unknown azure query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	if conf.AzureConfig.AccountName == "" {
		return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountNameParam)
	}
	if conf.AzureConfig.AccountKey == "" {
		return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountKeyParam)
	}
	if conf.AzureConfig.Environment == "" {
		// Default to AzurePublicCloud if not specified for backwards compatibility
		conf.AzureConfig.Environment = azure.PublicCloud.Name
	}
	conf.AzureConfig.Prefix = strings.TrimLeft(conf.AzureConfig.Prefix, "/")
	return conf, nil
}

type azureStorage struct {
	conf      *cloudpb.ExternalStorage_Azure
	ioConf    base.ExternalIODirConfig
	container *container.Client
	prefix    string
	settings  *cluster.Settings
}

var _ cloud.ExternalStorage = &azureStorage{}

func makeAzureStorage(
	_ context.Context, args cloud.ExternalStorageContext, dest cloudpb.ExternalStorage,
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
	env, err := azure.EnvironmentFromName(conf.Environment)
	if err != nil {
		return nil, errors.Wrap(err, "azure environment")
	}
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.%s", conf.AccountName, env.StorageEndpointSuffix))
	if err != nil {
		return nil, errors.Wrap(err, "azure: account name is not valid")
	}

	azClient, err := service.NewClientWithSharedKeyCredential(u.String(), credential, nil)
	if err != nil {
		return nil, err
	}

	return &azureStorage{
		conf:      conf,
		ioConf:    args.IOConf,
		container: azClient.NewContainerClient(conf.Container),
		prefix:    conf.Prefix,
		settings:  args.Settings,
	}, nil
}

func (s *azureStorage) getBlob(basename string) *blockblob.Client {
	name := path.Join(s.prefix, basename)
	return s.container.NewBlockBlobClient(name)
}

func (s *azureStorage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{
		Provider:    cloudpb.ExternalStorageProvider_azure,
		AzureConfig: s.conf,
	}
}

func (s *azureStorage) ExternalIOConf() base.ExternalIODirConfig {
	return s.ioConf
}

func (s *azureStorage) RequiresExternalIOAccounting() bool { return true }

func (s *azureStorage) Settings() *cluster.Settings {
	return s.settings
}

func (s *azureStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	ctx, sp := tracing.ChildSpan(ctx, "azure.Writer")
	sp.SetTag("path", attribute.StringValue(path.Join(s.prefix, basename)))
	blob := s.getBlob(basename)
	return cloud.BackgroundPipe(ctx, func(ctx context.Context, r io.Reader) error {
		defer sp.Finish()
		_, err := blob.UploadStream(ctx, r, &azblob.UploadStreamOptions{
			BlockSize:   cloud.WriteChunkSize.Get(&s.settings.SV),
			Concurrency: int(maxConcurrentUploadBuffers.Get(&s.settings.SV)),
		})
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
	sp.SetTag("path", attribute.StringValue(path.Join(s.prefix, basename)))
	resp, err := s.getBlob(basename).DownloadStream(ctx, &azblob.DownloadStreamOptions{Range: azblob.
		HTTPRange{Offset: offset}})
	if azerr := (*azcore.ResponseError)(nil); errors.As(err, &azerr) {
		if azerr.ErrorCode == "BlobNotFound" {
			// nolint:errwrap
			return nil, 0, errors.Wrapf(
				errors.Wrap(cloud.ErrFileDoesNotExist, "azure blob does not exist"),
				"%v",
				err.Error(),
			)
		}
		return nil, 0, err
	}
	var size int64
	if offset == 0 {
		size = *resp.ContentLength
	} else {
		size, err = cloud.CheckHTTPContentRangeHeader(*resp.ContentRange, offset)
		if err != nil {
			return nil, 0, err
		}
	}
	reader := resp.NewRetryReader(ctx, &azblob.RetryReaderOptions{MaxRetries: 3})
	return ioctx.ReadCloserAdapter(reader), size, nil
}

func (s *azureStorage) List(ctx context.Context, prefix, delim string, fn cloud.ListingFn) error {
	ctx, sp := tracing.ChildSpan(ctx, "azure.List")
	defer sp.Finish()

	dest := cloud.JoinPathPreservingTrailingSlash(s.prefix, prefix)
	sp.SetTag("path", attribute.StringValue(dest))

	pager := s.container.NewListBlobsHierarchyPager(delim, &container.ListBlobsHierarchyOptions{Prefix: &dest})
	for pager.More() {
		response, err := pager.NextPage(ctx)

		if err != nil {
			return errors.Wrap(err, "unable to list files for specified blob")
		}
		for _, blob := range response.Segment.BlobPrefixes {
			if err := fn(strings.TrimPrefix(*blob.Name, dest)); err != nil {
				return err
			}
		}
		for _, blob := range response.Segment.BlobItems {
			if err := fn(strings.TrimPrefix(*blob.Name, dest)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *azureStorage) Delete(ctx context.Context, basename string) error {
	err := contextutil.RunWithTimeout(ctx, "delete azure file", cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			_, err := s.getBlob(basename).Delete(ctx, nil)
			return err
		})
	return errors.Wrap(err, "delete file")
}

func (s *azureStorage) Size(ctx context.Context, basename string) (int64, error) {
	var props blob.GetPropertiesResponse
	err := contextutil.RunWithTimeout(ctx, "size azure file", cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			var err error
			props, err = s.getBlob(basename).GetProperties(ctx, nil)
			return err
		})
	if err != nil {
		return 0, errors.Wrap(err, "get file properties")
	}
	return *props.ContentLength, nil
}

// Close is part of the cloud.ExternalStorage interface.
func (s *azureStorage) Close() error {
	return nil
}

func init() {
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_azure,
		parseAzureURL, makeAzureStorage, cloud.RedactedParams(AzureAccountKeyParam), scheme, deprecatedScheme, deprecatedExternalConnectionScheme)
}
