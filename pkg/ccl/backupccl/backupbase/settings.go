// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
		settings.TenantWritable,
		"bulkio.backup.merge_file_buffer_size",
		"size limit used when buffering backup files before merging them",
		int64(defaultSmallFileBuffer),
	)
)
