// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
