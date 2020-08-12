// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantrate

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// Limit defines a rate in units per second.
type Limit float64

// LimitConfig configures the rate limit and burst limit for a given resource.
type LimitConfig struct {
	Rate  Limit
	Burst int64
}

// LimitConfigs configures the rate limits.
// It is exported for convenience and testing.
// The values are derived from cluster settings.
type LimitConfigs struct {
	Requests   LimitConfig
	ReadBytes  LimitConfig
	WriteBytes LimitConfig
}

// LimitConfigsFromSettings constructs LimitConfigs from the values stored in
// the settings.
func LimitConfigsFromSettings(settings *cluster.Settings) LimitConfigs {
	return LimitConfigs{
		Requests: LimitConfig{
			Rate:  Limit(requestRateLimit.Get(&settings.SV)),
			Burst: requestBurstLimit.Get(&settings.SV),
		},
		ReadBytes: LimitConfig{
			Rate:  Limit(readRateLimit.Get(&settings.SV)),
			Burst: readBurstLimit.Get(&settings.SV),
		},
		WriteBytes: LimitConfig{
			Rate:  Limit(writeRateLimit.Get(&settings.SV)),
			Burst: writeBurstLimit.Get(&settings.SV),
		},
	}
}

var (
	requestRateLimit = settings.RegisterPositiveFloatSetting(
		"kv.tenant_rate_limiter.requests.rate_limit",
		"per-tenant request rate limit in requests per second",
		128)

	requestBurstLimit = settings.RegisterPositiveIntSetting(
		"kv.tenant_rate_limiter.request.burst_limit",
		"per-tenant request burst limit in requests",
		512)

	readRateLimit = settings.RegisterByteSizeSetting(
		"kv.tenant_rate_limiter.read_bytes.rate_limit",
		"per-tenant read rate limit in bytes per second",
		1<<20 /* 1 MiB */)

	readBurstLimit = settings.RegisterByteSizeSetting(
		"kv.tenant_rate_limiter.read_bytes.burst_limit",
		"per-tenant read burst limit in bytes",
		16<<20 /* 16 MiB */)

	writeRateLimit = settings.RegisterByteSizeSetting(
		"kv.tenant_rate_limiter.write_bytes.rate_limit",
		"per-tenant write rate limit in bytes per second",
		512<<10 /* 512 KiB */)

	writeBurstLimit = settings.RegisterByteSizeSetting(
		"kv.tenant_rate_limiter.write_bytes.burst_limit",
		"per-tenant write burst limit in bytes",
		8<<20 /* 8 MiB */)

	// settingsSetOnChangeFuncs are the functions used to register the factory to
	// be notified of changes to any of the settings which configure it.
	settingsSetOnChangeFuncs = [...]func(*settings.Values, func()){
		requestRateLimit.SetOnChange,
		requestBurstLimit.SetOnChange,
		readRateLimit.SetOnChange,
		readBurstLimit.SetOnChange,
		writeRateLimit.SetOnChange,
		writeBurstLimit.SetOnChange,
	}
)
