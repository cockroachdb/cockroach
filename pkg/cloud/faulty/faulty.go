// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package faulty

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
)

func WrapStorage(
	wrappedStorage cloud.ExternalStorage, hooks *cloud.TestingKnobs,
) cloud.ExternalStorage {
	if hooks == nil || wrappedStorage == nil {
		return wrappedStorage
	}
	opFaults := hooks.OpFaults
	if opFaults == nil {
		opFaults = fault.NopStrategy
	}
	ioFaults := hooks.IoFaults
	if ioFaults == nil {
		ioFaults = fault.NopStrategy
	}
	return &faultyStorage{
		wrappedStorage: wrappedStorage,
		opFaults:       opFaults,
		ioFaults:       ioFaults,
	}
}

// TODO(jeffswenson): we should adjust the op implementations so that we
// sometimes return an error for operations that succeed.
type faultyStorage struct {
	wrappedStorage cloud.ExternalStorage
	opFaults       fault.Strategy
	ioFaults       fault.Strategy
}

// Close implements cloud.ExternalStorage.
func (f *faultyStorage) Close() error {
	return f.wrappedStorage.Close()
}

// Conf implements cloud.ExternalStorage.
func (f *faultyStorage) Conf() cloudpb.ExternalStorage {
	return f.wrappedStorage.Conf()
}

func (f *faultyStorage) injectErr(ctx context.Context, opName string, basename string) error {
	if !f.opFaults.ShouldInject(ctx, opName) {
		return nil
	}
	return errors.Newf("%s failed: injected error for '%s'", opName, basename)
}

// Delete implements cloud.ExternalStorage.
func (f *faultyStorage) Delete(ctx context.Context, basename string) error {
	if err := f.injectErr(ctx, "externalstorage.delete", basename); err != nil {
		return err
	}
	return f.wrappedStorage.Delete(ctx, basename)
}

// ExternalIOConf implements cloud.ExternalStorage.
func (f *faultyStorage) ExternalIOConf() base.ExternalIODirConfig {
	return f.wrappedStorage.ExternalIOConf()
}

// List implements cloud.ExternalStorage.
func (f *faultyStorage) List(
	ctx context.Context, prefix string, delimiter string, fn cloud.ListingFn,
) error {
	if err := f.injectErr(ctx, "externalstorage.list", prefix); err != nil {
		return err
	}
	return f.wrappedStorage.List(ctx, prefix, delimiter, fn)
}

// ReadFile implements cloud.ExternalStorage.
func (f *faultyStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (_ ioctx.ReadCloserCtx, fileSize int64, _ error) {
	// TODO(jeffswenson): we should also wrap the ReadCloserCtx so that we
	// sometimes return an error after opening successfully.
	if err := f.injectErr(ctx, "externalstorage.read", basename); err != nil {
		return nil, 0, err
	}
	reader, size, err := f.wrappedStorage.ReadFile(ctx, basename, opts)
	if err != nil {
		return nil, 0, err
	}
	return &faultyReader{
		wrappedReader: reader,
		ioFaults:      f.ioFaults,
		opFaults:      f.opFaults,
		basename:      basename,
	}, size, nil
}

// RequiresExternalIOAccounting implements cloud.ExternalStorage.
func (f *faultyStorage) RequiresExternalIOAccounting() bool {
	return f.wrappedStorage.RequiresExternalIOAccounting()
}

// Settings implements cloud.ExternalStorage.
func (f *faultyStorage) Settings() *cluster.Settings {
	return f.wrappedStorage.Settings()
}

// Size implements cloud.ExternalStorage.
func (f *faultyStorage) Size(ctx context.Context, basename string) (int64, error) {
	if err := f.injectErr(ctx, "externalstorage.size", basename); err != nil {
		return 0, err
	}
	return f.wrappedStorage.Size(ctx, basename)
}

// Writer implements cloud.ExternalStorage.
func (f *faultyStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	if err := f.injectErr(ctx, "externalstorage.writer", basename); err != nil {
		return nil, err
	}
	writer, err := f.wrappedStorage.Writer(ctx, basename)
	if err != nil {
		return nil, err
	}
	return &faultyWriter{
		ctx:           ctx,
		wrappedWriter: writer,
		ioFaults:      f.ioFaults,
		opFaults:      f.opFaults,
		basename:      basename,
	}, nil
}

var _ cloud.ExternalStorage = &faultyStorage{}
