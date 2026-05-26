// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import "github.com/cockroachdb/cockroach/pkg/base"

// ExternalStorageOption is an option passed during the construction
// of an external storage.
type ExternalStorageOption func(opts *ExternalStorageOptions)

// WithIOAccountingInterceptor sets the ReadWriterInterceptor that is used for IO Accounting.
func WithIOAccountingInterceptor(i ReadWriterInterceptor) ExternalStorageOption {
	return func(opts *ExternalStorageOptions) {
		opts.ioAccountingInterceptor = i
	}
}

func WithAzureStorageTestingKnobs(knobs base.ModuleTestingKnobs) ExternalStorageOption {
	return func(opts *ExternalStorageOptions) {
		opts.AzureStorageTestingKnobs = knobs
	}
}

// WithClientName sets the "client" label on network metrics.
func WithClientName(name string) ExternalStorageOption {
	return func(opts *ExternalStorageOptions) {
		opts.ClientName = name
	}
}
