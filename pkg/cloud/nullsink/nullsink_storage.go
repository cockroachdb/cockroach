// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nullsink

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
)

func parseNullURL(_ cloud.ExternalStorageURIContext, _ *url.URL) (cloudpb.ExternalStorage, error) {
	return cloudpb.ExternalStorage{Provider: cloudpb.ExternalStorageProvider_null}, nil
}

// NullRequiresExternalIOAccounting is the return falues for
// (*nullSinkStorage).RequiresExternalIOAccounting. This is exposed for testing.
var NullRequiresExternalIOAccounting = false

// MakeNullSinkStorageURI returns a valid null sink URI.
func MakeNullSinkStorageURI(path string) string {
	return fmt.Sprintf("null:///%s", path)
}

type nullSinkStorage struct {
}

var _ cloud.ExternalStorage = &nullSinkStorage{}

func makeNullSinkStorage(
	_ context.Context, _ cloud.ExternalStorageContext, _ cloudpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.nullsink")
	return &nullSinkStorage{}, nil
}

func (n *nullSinkStorage) Close() error {
	return nil
}

func (n *nullSinkStorage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{Provider: cloudpb.ExternalStorageProvider_null}
}

func (n *nullSinkStorage) ExternalIOConf() base.ExternalIODirConfig {
	return base.ExternalIODirConfig{}
}

func (n *nullSinkStorage) RequiresExternalIOAccounting() bool {
	return NullRequiresExternalIOAccounting
}

func (n *nullSinkStorage) Settings() *cluster.Settings {
	return nil
}

func (n *nullSinkStorage) ReadFile(
	_ context.Context, _ string, _ cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	return nil, 0, io.EOF
}

type nullWriter struct{}

func (nullWriter) Write(p []byte) (int, error) { return len(p), nil }
func (nullWriter) Close() error                { return nil }

func (n *nullSinkStorage) Writer(_ context.Context, _ string) (io.WriteCloser, error) {
	return nullWriter{}, nil
}

func (n *nullSinkStorage) List(_ context.Context, _, _ string, _ cloud.ListingFn) error {
	return nil
}

func (n *nullSinkStorage) Delete(_ context.Context, _ string) error {
	return nil
}

func (n *nullSinkStorage) Size(_ context.Context, _ string) (int64, error) {
	return 0, nil
}

var _ cloud.ExternalStorage = &nullSinkStorage{}

func init() {
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_null,
		cloud.RegisteredProvider{
			ConstructFn:    makeNullSinkStorage,
			ParseFn:        parseNullURL,
			RedactedParams: cloud.RedactedParams(),
			Schemes:        []string{"null"},
		})
}
