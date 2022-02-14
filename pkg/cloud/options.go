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

// ExternalStorageOption is an option passed during the construction
// of an external storage.
type ExternalStorageOption func(ctx *ExternalStorageContext)

// WithReadWriterInterceptor is an option that will install the given
// ReadWriteInterceptor for every Reader or Writer returned by the
// External Storage.
func WithReadWriterInterceptor(i ReadWriterInterceptor) ExternalStorageOption {
	return func(ctx *ExternalStorageContext) {
		ctx.rwInterceptor = i
	}
}
