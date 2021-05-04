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
	"net/url"
	"path"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/errors"
)

func parseAzureURL(_ ExternalStorageURIContext, uri *url.URL) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	conf.Provider = roachpb.ExternalStorageProvider_Azure
	conf.AzureConfig = &roachpb.ExternalStorage_Azure{
		Container:   uri.Host,
		Prefix:      uri.Path,
		AccountName: uri.Query().Get(AzureAccountNameParam),
		AccountKey:  uri.Query().Get(AzureAccountKeyParam),
		/* NB: additions here should also update azureQueryParams() serializer */
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

func azureQueryParams(conf *roachpb.ExternalStorage_Azure) string {
	q := make(url.Values)
	if conf.AccountName != "" {
		q.Set(AzureAccountNameParam, conf.AccountName)
	}
	if conf.AccountKey != "" {
		q.Set(AzureAccountKeyParam, conf.AccountKey)
	}
	return q.Encode()
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
	_ context.Context, args ExternalStorageContext, dest roachpb.ExternalStorage,
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
		Provider:    roachpb.ExternalStorageProvider_Azure,
		AzureConfig: s.conf,
	}
}

func (s *azureStorage) ExternalIOConf() base.ExternalIODirConfig {
	return s.ioConf
}

func (s *azureStorage) Settings() *cluster.Settings {
	return s.settings
}

func (s *azureStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	err := contextutil.RunWithTimeout(ctx, "write azure file", timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			_, err := blob.Upload(
				ctx, content, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{},
				azblob.DefaultAccessTier, nil /* blobTagsMap */, azblob.ClientProvidedKeyOptions{},
			)
			return err
		})
	return errors.Wrapf(err, "write file: %s", basename)
}

// ReadFile is shorthand for ReadFileAt with offset 0.
func (s *azureStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	reader, _, err := s.ReadFileAt(ctx, basename, 0)
	return reader, err
}

func (s *azureStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (io.ReadCloser, int64, error) {
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
				return nil, 0, errors.Wrapf(cloud.ErrFileDoesNotExist, "azure blob does not exist: %s", err.Error())
			}
		}
		return nil, 0, errors.Wrap(err, "failed to create azure reader")
	}
	var size int64
	if offset == 0 {
		size = get.ContentLength()
	} else {
		size, err = checkHTTPContentRangeHeader(get.ContentRange(), offset)
		if err != nil {
			return nil, 0, err
		}
	}
	reader := get.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3})

	return reader, size, nil
}

func (s *azureStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	pattern := s.prefix
	if patternSuffix != "" {
		if containsGlob(s.prefix) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = path.Join(pattern, patternSuffix)
	}
	var fileList []string
	response, err := s.container.ListBlobsFlatSegment(ctx,
		azblob.Marker{},
		azblob.ListBlobsSegmentOptions{Prefix: getPrefixBeforeWildcard(s.prefix)},
	)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list files for specified blob")
	}

	for _, blob := range response.Segment.BlobItems {
		matches, err := path.Match(pattern, blob.Name)
		if err != nil {
			continue
		}
		if matches {
			if patternSuffix != "" {
				if !strings.HasPrefix(blob.Name, s.prefix) {
					// TODO(dt): return a nice rel-path instead of erroring out.
					return nil, errors.New("pattern matched file outside of path")
				}
				fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(blob.Name, s.prefix), "/"))
			} else {
				azureURL := url.URL{
					Scheme:   "azure",
					Host:     strings.TrimPrefix(s.container.URL().Path, "/"),
					Path:     blob.Name,
					RawQuery: azureQueryParams(s.conf),
				}
				fileList = append(fileList, azureURL.String())
			}
		}
	}

	return fileList, nil
}

func (s *azureStorage) Delete(ctx context.Context, basename string) error {
	err := contextutil.RunWithTimeout(ctx, "delete azure file", timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			return err
		})
	return errors.Wrap(err, "delete file")
}

func (s *azureStorage) Size(ctx context.Context, basename string) (int64, error) {
	var props *azblob.BlobGetPropertiesResponse
	err := contextutil.RunWithTimeout(ctx, "size azure file", timeoutSetting.Get(&s.settings.SV),
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

func (s *azureStorage) Close() error {
	return nil
}
