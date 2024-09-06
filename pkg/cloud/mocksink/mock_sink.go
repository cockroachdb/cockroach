// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mocksink

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
)

// mockSinkStorage can be useful for testing to override the WriteCloser.
type mockSinkStorage struct {
	writer func() io.WriteCloser
}

var _ cloud.ExternalStorage = &mockSinkStorage{}

func MakeMockSinkStorage(writer func() io.WriteCloser) cloud.ExternalStorage {
	return &mockSinkStorage{writer: writer}
}

func (n *mockSinkStorage) Close() error {
	return nil
}

func (n *mockSinkStorage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{Provider: cloudpb.ExternalStorageProvider_null}
}

func (n *mockSinkStorage) ExternalIOConf() base.ExternalIODirConfig {
	return base.ExternalIODirConfig{}
}

func (n *mockSinkStorage) RequiresExternalIOAccounting() bool {
	return false
}

func (n *mockSinkStorage) Settings() *cluster.Settings {
	return nil
}

func (n *mockSinkStorage) ReadFile(
	_ context.Context, _ string, _ cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	return nil, 0, io.EOF
}

func (n *mockSinkStorage) Writer(_ context.Context, _ string) (io.WriteCloser, error) {
	return n.writer(), nil
}

func (n *mockSinkStorage) List(_ context.Context, _, _ string, _ cloud.ListingFn) error {
	return nil
}

func (n *mockSinkStorage) Delete(_ context.Context, _ string) error {
	return nil
}

func (n *mockSinkStorage) Size(_ context.Context, _ string) (int64, error) {
	return 0, nil
}
