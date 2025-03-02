// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
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
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/attribute"
)

var maxConcurrentUploadBuffers = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"cloudstorage.azure.concurrent_upload_buffers",
	"controls the number of concurrent buffers that will be used by the Azure client when uploading chunks."+
		"Each buffer can buffer up to cloudstorage.write_chunk.size of memory during an upload",
	1,
	settings.WithPublic)

// A note on Azure authentication:
//
// The standardized way to authenticate a third-party identity to the Azure
// Cloud is via an "App Registration." This is the equivalent of an ID/Secret
// pair on other providers, and uses the Azure-wide RBAC authentication
// system.
//
// Azure RBAC is supported across all Azure products, but is often not the
// only way to attach permissions. Individual Azure products often each
// provide their _own_ permissions systems, likely for legacy reasons.
//
// In the case of Azure storage, one can also authenticate using a "key" tied
// specifically to that storage account. (This key grants no other access,
// and cannot be modified or restricted in scope.)
//
// Were we building Azure support in CRDB from scratch, we probably would not
// support this access method. For backwards compatibility, however, we retain
// support for these keys on Storage.
//
// So to authenticate manually to Azure Storage, a CRDB user must provide
// EITHER:
// 1. AZURE_ACCOUNT_KEY (legacy storage-key access), OR
// 2. All three of AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID (RBAC
// access).
// Alternatively, a user may
// 3. Use implicit authentication with a managed identity derived from the
// environment on an Azure cluster host.
const (
	// AzureAccountNameParam is the query parameter for account_name in an azure URI.
	// Specifically, this is one half of a "bucket" identifier. The other half is the
	// Azure Container name, supplied in the host field of the storage URL.
	// These two items together uniquely definte an Azure Blob.
	AzureAccountNameParam = "AZURE_ACCOUNT_NAME"
	// AzureEnvironmentKeyParam is the query parameter for the environment name in an azure URI.
	AzureEnvironmentKeyParam = "AZURE_ENVIRONMENT"

	// Storage Key identifiers:

	// AzureAccountKeyParam is the query parameter for account_key in an azure URI.
	AzureAccountKeyParam = "AZURE_ACCOUNT_KEY"

	// App Registration identifiers:

	// AzureClientIDParam is the query parameter for client_id in an azure URI.
	AzureClientIDParam = "AZURE_CLIENT_ID"
	// AzureClientSecretParam is the query parameter for client_secret in an azure URI.
	AzureClientSecretParam = "AZURE_CLIENT_SECRET"
	// AzureTenantIDParam is the query parameter for tenant_id in an azure URI.
	// Note that tenant ID here refers to the Azure Active Directory tenant,
	// _not_ to any CRDB tenant.
	AzureTenantIDParam = "AZURE_TENANT_ID"

	scheme = "azure-blob"

	deprecatedScheme                   = "azure"
	deprecatedExternalConnectionScheme = "azure-storage"
)

func azureAuthMethod(uri *url.URL, consumeURI *cloud.ConsumeURL) (cloudpb.AzureAuth, error) {
	authParam := consumeURI.ConsumeParam(cloud.AuthParam)
	switch authParam {
	case "", cloud.AuthParamSpecified:
		if uri.Query().Get(AzureAccountKeyParam) != "" {
			return cloudpb.AzureAuth_LEGACY, nil
		}
		return cloudpb.AzureAuth_EXPLICIT, nil
	case cloud.AuthParamImplicit:
		return cloudpb.AzureAuth_IMPLICIT, nil
	default:
		return 0, errors.Errorf("unsupported value %s for %s",
			authParam, cloud.AuthParam)
	}

}

func parseAzureURL(uri *url.URL) (cloudpb.ExternalStorage, error) {
	azureURL := cloud.ConsumeURL{URL: uri}
	conf := cloudpb.ExternalStorage{}
	conf.Provider = cloudpb.ExternalStorageProvider_azure
	auth, err := azureAuthMethod(uri, &azureURL)
	if err != nil {
		return conf, err
	}
	conf.AzureConfig = &cloudpb.ExternalStorage_Azure{
		Container:    uri.Host,
		Prefix:       uri.Path,
		AccountName:  azureURL.ConsumeParam(AzureAccountNameParam),
		AccountKey:   azureURL.ConsumeParam(AzureAccountKeyParam),
		Environment:  azureURL.ConsumeParam(AzureEnvironmentKeyParam),
		ClientID:     azureURL.ConsumeParam(AzureClientIDParam),
		ClientSecret: azureURL.ConsumeParam(AzureClientSecretParam),
		TenantID:     azureURL.ConsumeParam(AzureTenantIDParam),
		Auth:         auth,
	}

	// Validate that all the passed in parameters are supported.
	if unknownParams := azureURL.RemainingQueryParams(); len(unknownParams) > 0 {
		return cloudpb.ExternalStorage{}, errors.Errorf(
			`unknown azure query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	if conf.AzureConfig.AccountName == "" {
		return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountNameParam)
	}

	const explicitErrMsg = "explicit azure uri requires exactly one authentication method: %q OR all three of %q, %q, and %q"
	// Validate the authentication parameters are set correctly.
	switch conf.AzureConfig.Auth {
	case cloudpb.AzureAuth_LEGACY:
		hasKeyCred := conf.AzureConfig.AccountKey != ""
		hasARoleCred := conf.AzureConfig.TenantID != "" || conf.AzureConfig.ClientID != "" || conf.AzureConfig.ClientSecret != ""
		if !hasKeyCred || hasARoleCred {
			// If the URI params are misconfigured we can't be certain Legacy Auth
			// was intended, so print a broader error message.
			return conf, errors.Errorf(explicitErrMsg, AzureAccountKeyParam, AzureTenantIDParam, AzureClientIDParam, AzureClientSecretParam)
		}
	case cloudpb.AzureAuth_EXPLICIT:
		hasKeyCred := conf.AzureConfig.AccountKey != ""
		hasAllRoleCreds := conf.AzureConfig.TenantID != "" && conf.AzureConfig.ClientID != "" && conf.AzureConfig.ClientSecret != ""
		if hasKeyCred || !hasAllRoleCreds {
			// If the URI params are misconfigured we can't be certain Explicit Auth
			// was intended, so print a broader error message.
			return conf, errors.Errorf(explicitErrMsg, AzureAccountKeyParam, AzureTenantIDParam, AzureClientIDParam, AzureClientSecretParam)
		}
	case cloudpb.AzureAuth_IMPLICIT:
		unsupportedParams := make([]string, 0)
		if conf.AzureConfig.AccountKey != "" {
			unsupportedParams = append(unsupportedParams, AzureAccountKeyParam)
		}
		if conf.AzureConfig.TenantID != "" {
			unsupportedParams = append(unsupportedParams, AzureTenantIDParam)
		}
		if conf.AzureConfig.ClientID != "" {
			unsupportedParams = append(unsupportedParams, AzureClientIDParam)
		}
		if conf.AzureConfig.ClientSecret != "" {
			unsupportedParams = append(unsupportedParams, AzureClientSecretParam)
		}
		if len(unsupportedParams) > 0 {
			return conf, errors.Errorf("implicit azure auth does not support uri param %s", strings.Join(unsupportedParams, ", "))
		}
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
	_ context.Context, args cloud.EarlyBootExternalStorageContext, dest cloudpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.azure")
	conf := dest.AzureConfig
	if conf == nil {
		return nil, errors.Errorf("azure upload requested but info missing")
	}
	env, err := azure.EnvironmentFromName(conf.Environment)
	if err != nil {
		return nil, errors.Wrap(err, "azure environment")
	}
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.%s", conf.AccountName, env.StorageEndpointSuffix))
	if err != nil {
		return nil, errors.Wrap(err, "azure: account name is not valid")
	}

	options := args.ExternalStorageOptions()
	t, err := cloud.MakeHTTPClient(args.Settings, args.MetricsRecorder, "azure", dest.AzureConfig.Container, options.ClientName)
	if err != nil {
		return nil, errors.Wrap(err, "azure: unable to create transport")
	}
	var opts service.ClientOptions
	opts.Transport = t

	var azClient *service.Client
	switch conf.Auth {
	case cloudpb.AzureAuth_LEGACY:
		credential, err := azblob.NewSharedKeyCredential(conf.AccountName, conf.AccountKey)
		if err != nil {
			return nil, errors.Wrap(err, "azure shared key credential")
		}
		azClient, err = service.NewClientWithSharedKeyCredential(u.String(), credential, &opts)
		if err != nil {
			return nil, err
		}
	case cloudpb.AzureAuth_EXPLICIT:
		credential, err := azidentity.NewClientSecretCredential(conf.TenantID, conf.ClientID, conf.ClientSecret, nil)
		if err != nil {
			return nil, errors.Wrap(err, "azure client secret credential")
		}
		azClient, err = service.NewClient(u.String(), credential, &opts)
		if err != nil {
			return nil, err
		}
	case cloudpb.AzureAuth_IMPLICIT:
		if args.IOConf.DisableImplicitCredentials {
			return nil, errors.New(
				"implicit credentials disallowed for azure due to --external-io-disable-implicit-credentials flag")
		}

		defaultCredentialsOptions := &DefaultAzureCredentialWithFileOptions{}
		if knobs := options.AzureStorageTestingKnobs; knobs != nil {
			defaultCredentialsOptions.testingKnobs = knobs.(*TestingKnobs)
		}

		credential, err := NewDefaultAzureCredentialWithFile(defaultCredentialsOptions)
		if err != nil {
			return nil, errors.Wrap(err, "azure default credential")
		}
		azClient, err = service.NewClient(u.String(), credential, &opts)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unsupported value %s for %s", conf.Auth, cloud.AuthParam)
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

// isNotFoundErr checks if the error indicates a blob not found condition.
func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	var azerr *azcore.ResponseError
	return errors.As(err, &azerr) && azerr.ErrorCode == "BlobNotFound"
}

func (s *azureStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (_ ioctx.ReadCloserCtx, fileSize int64, _ error) {
	ctx, sp := tracing.ChildSpan(ctx, "azure.ReadFile")
	defer sp.Finish()
	sp.SetTag("path", attribute.StringValue(path.Join(s.prefix, basename)))
	resp, err := s.getBlob(basename).DownloadStream(ctx, &azblob.DownloadStreamOptions{Range: azblob.
		HTTPRange{Offset: opts.Offset}})
	if err != nil {
		if isNotFoundErr(err) {
			return nil, 0, cloud.WrapErrFileDoesNotExist(err, "azure blob does not exist")
		}
		return nil, 0, errors.Wrapf(err, "failed to create azure reader")
	}

	if !opts.NoFileSize {
		if opts.Offset == 0 {
			fileSize = *resp.ContentLength
		} else {
			fileSize, err = cloud.CheckHTTPContentRangeHeader(*resp.ContentRange, opts.Offset)
			if err != nil {
				return nil, 0, err
			}
		}
	}
	reader := resp.NewRetryReader(ctx, &azblob.RetryReaderOptions{MaxRetries: 3})
	return ioctx.ReadCloserAdapter(reader), fileSize, nil
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
	err := timeutil.RunWithTimeout(ctx, "delete azure file", cloud.Timeout.Get(&s.settings.SV),
		func(ctx context.Context) error {
			_, err := s.getBlob(basename).Delete(ctx, nil)
			if isNotFoundErr(err) {
				return nil
			}
			return err
		})
	return errors.Wrap(err, "delete file")
}

func (s *azureStorage) Size(ctx context.Context, basename string) (int64, error) {
	var props blob.GetPropertiesResponse
	err := timeutil.RunWithTimeout(ctx, "size azure file", cloud.Timeout.Get(&s.settings.SV),
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

type TestingKnobs struct {
	MapFileCredentialToken func(azcore.AccessToken, error) (azcore.AccessToken, error)
}

func (*TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

func init() {
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_azure,
		cloud.RegisteredProvider{
			EarlyBootConstructFn: makeAzureStorage,
			EarlyBootParseFn:     parseAzureURL,
			RedactedParams:       cloud.RedactedParams(AzureAccountKeyParam),
			Schemes:              []string{scheme, deprecatedScheme, deprecatedExternalConnectionScheme},
		})
}
