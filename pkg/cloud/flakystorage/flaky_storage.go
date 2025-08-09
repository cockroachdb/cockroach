package flakystorage

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TODO(jeffswenson): we should adjust the op implementations so that we
// sometimes return an error for operations that succeed.
type flakyStorage struct {
	wrappedStorage cloud.ExternalStorage
	shouldInject   func() bool
}

// Close implements cloud.ExternalStorage.
func (f *flakyStorage) Close() error {
	return f.wrappedStorage.Close()
}

// Conf implements cloud.ExternalStorage.
func (f *flakyStorage) Conf() cloudpb.ExternalStorage {
	return f.wrappedStorage.Conf()
}

func (f *flakyStorage) injectErr(ctx context.Context, opName string, basename string) error {
	if !f.shouldInject() {
		return nil

	}
	log.Infof(ctx, "injected error for %s %s", opName, basename)
	return errors.Newf("%s failed: injected error for '%s'", opName, basename)
}

// Delete implements cloud.ExternalStorage.
func (f *flakyStorage) Delete(ctx context.Context, basename string) error {
	if err := f.injectErr(ctx, "externalstorage.delete", basename); err != nil {
		return err
	}
	return f.wrappedStorage.Delete(ctx, basename)
}

// ExternalIOConf implements cloud.ExternalStorage.
func (f *flakyStorage) ExternalIOConf() base.ExternalIODirConfig {
	return f.wrappedStorage.ExternalIOConf()
}

// List implements cloud.ExternalStorage.
func (f *flakyStorage) List(
	ctx context.Context, prefix string, delimiter string, fn cloud.ListingFn,
) error {
	if err := f.injectErr(ctx, "externalstorage.list", prefix); err != nil {
		return err
	}
	return f.wrappedStorage.List(ctx, prefix, delimiter, fn)
}

// ReadFile implements cloud.ExternalStorage.
func (f *flakyStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (_ ioctx.ReadCloserCtx, fileSize int64, _ error) {
	// TODO(jeffswenson): we should also wrap the ReadCloserCtx so that we
	// sometimes return an error after opening successfully.
	if err := f.injectErr(ctx, "externalstorage.read", basename); err != nil {
		return nil, 0, err
	}
	return f.wrappedStorage.ReadFile(ctx, basename, opts)
}

// RequiresExternalIOAccounting implements cloud.ExternalStorage.
func (f *flakyStorage) RequiresExternalIOAccounting() bool {
	return f.wrappedStorage.RequiresExternalIOAccounting()
}

// Settings implements cloud.ExternalStorage.
func (f *flakyStorage) Settings() *cluster.Settings {
	return f.wrappedStorage.Settings()
}

// Size implements cloud.ExternalStorage.
func (f *flakyStorage) Size(ctx context.Context, basename string) (int64, error) {
	if err := f.injectErr(ctx, "externalstorage.size", basename); err != nil {
		return 0, err
	}
	return f.wrappedStorage.Size(ctx, basename)
}

// Writer implements cloud.ExternalStorage.
func (f *flakyStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	if err := f.injectErr(ctx, "externalstorage.writer", basename); err != nil {
		return nil, err
	}
	// TODO(jeffswenson): we should also wrap the WriteCloser so that we
	// sometimes return an error after opening successfully.
	return f.wrappedStorage.Writer(ctx, basename)
}

var _ cloud.ExternalStorage = &flakyStorage{}

// TODO: create a setting that is a bucket regex. Buckets matching the regex
// will be wrapped with a flaky storage wrapper.

// TODO: define an error frequency setting which is the amount of time between
// each error.

// TODO: define an error probability setting which is the probability of an
// error when errors are enabled.
