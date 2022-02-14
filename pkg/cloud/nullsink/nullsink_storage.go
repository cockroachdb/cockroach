// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nullsink

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
)

func parseNullURL(_ cloud.ExternalStorageURIContext, _ *url.URL) (roachpb.ExternalStorage, error) {
	return roachpb.ExternalStorage{Provider: roachpb.ExternalStorageProvider_null}, nil
}

// MakeNullSinkStorageURI returns a valid null sink URI.
func MakeNullSinkStorageURI(path string) string {
	return fmt.Sprintf("null:///%s", path)
}

type nullSinkStorage struct {
}

var _ cloud.ExternalStorage = &nullSinkStorage{}

func makeNullSinkStorage(
	_ context.Context, _ cloud.ExternalStorageContext, _ roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.nullsink")
	return &nullSinkStorage{}, nil
}

func (n *nullSinkStorage) Close() error {
	return nil
}

func (n *nullSinkStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{Provider: roachpb.ExternalStorageProvider_null}
}

func (n *nullSinkStorage) ExternalIOConf() base.ExternalIODirConfig {
	return base.ExternalIODirConfig{}
}

func (n *nullSinkStorage) Settings() *cluster.Settings {
	return nil
}

func (n *nullSinkStorage) ReadFile(
	ctx context.Context, basename string,
) (ioctx.ReadCloserCtx, error) {
	reader, _, err := n.ReadFileAt(ctx, basename, 0)
	return reader, err
}

func (n *nullSinkStorage) ReadFileAt(
	_ context.Context, _ string, _ int64,
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
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_null,
		parseNullURL, makeNullSinkStorage, cloud.RedactedParams(), "null")
}
