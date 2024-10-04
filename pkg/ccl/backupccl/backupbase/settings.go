// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupbase

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
)

var (
	defaultSmallFileBuffer = util.ConstantWithMetamorphicTestRange(
		"backup-merge-file-buffer-size",
		128<<20, /* defaultValue */
		1<<20,   /* metamorphic min */
		16<<20,  /* metamorphic max */
	)

	// SmallFileBuffer is the size limit used when buffering backup files before
	// merging them.
	SmallFileBuffer = settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"bulkio.backup.merge_file_buffer_size",
		"size limit used when buffering backup files before merging them",
		int64(defaultSmallFileBuffer),
	)
)
