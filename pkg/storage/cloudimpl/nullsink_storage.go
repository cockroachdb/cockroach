// Copyright 2021 The Cockroach Authors.
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
	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
)

// MakeNullSinkStorageURI returns a valid null sink URI.
func MakeNullSinkStorageURI(path string) string {
	return fmt.Sprintf("null:///%s", path)
}

type nullSinkStorage struct {
}

func makeNullSinkStorage() (cloud.ExternalStorage, error) {
	return &nullSinkStorage{}, nil
}

func (n *nullSinkStorage) Close() error {
	return nil
}

func (n *nullSinkStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{Provider: roachpb.ExternalStorageProvider_NullSink}
}

func (n *nullSinkStorage) ExternalIOConf() base.ExternalIODirConfig {
	return base.ExternalIODirConfig{}
}

func (n *nullSinkStorage) Settings() *cluster.Settings {
	return nil
}

func (n *nullSinkStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	reader, _, err := n.ReadFileAt(ctx, basename, 0)
	return reader, err
}

func (n *nullSinkStorage) ReadFileAt(
	_ context.Context, _ string, _ int64,
) (io.ReadCloser, int64, error) {
	return nil, 0, io.EOF
}

func (n *nullSinkStorage) WriteFile(_ context.Context, _ string, content io.ReadSeeker) error {
	_, err := io.Copy(ioutil.Discard, content)
	return err
}

func (n *nullSinkStorage) ListFiles(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (n *nullSinkStorage) Delete(_ context.Context, _ string) error {
	return nil
}

func (n *nullSinkStorage) Size(_ context.Context, _ string) (int64, error) {
	return 0, nil
}

var _ cloud.ExternalStorage = &nullSinkStorage{}
