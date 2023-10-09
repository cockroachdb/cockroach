// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdcevent

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// RowFetcherTraceKVs controls if we write the experimental new format BACKUP
// metadata file.
var RowFetcherTraceKVs = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"changefeed.cdcevent.trace_kv",
	"enables pretty KV generation in the row fetcher so KVs can "+
		"be logged",
	false,
)

// RowFetcherTraceKVLogFrequency controls how frequently KVs are logged when
// KV tracing is enabled.
var RowFetcherTraceKVLogFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.cdcevent.trace_kv.log_frequency",
	"controls how frequently KVs are logged when KV tracing is enabled",
	1*time.Second,
	settings.NonNegativeDuration,
)
