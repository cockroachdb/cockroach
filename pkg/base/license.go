// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package base exposes basic utilities used across cockroach.
package base

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var LicenseTTLMetadata = metric.Metadata{
	// This metric name isn't namespaced for backwards
	// compatibility. The prior version of this metric was manually
	// inserted into the prometheus output
	Name:        "seconds_until_enterprise_license_expiry",
	Help:        "Seconds until license expiry (0 if no license present)",
	Measurement: "Seconds",
	Unit:        metric.Unit_SECONDS,
	Visibility:  metric.Metadata_ESSENTIAL,
	Category:    metric.Metadata_EXPIRATIONS,
	HowToUse:    "See Description.",
}

var AdditionalLicenseTTLMetadata = metric.Metadata{
	Name:        "seconds_until_license_expiry",
	Help:        "Seconds until license expiry (0 if no license present)",
	Measurement: "Seconds",
	Unit:        metric.Unit_SECONDS,
	Visibility:  metric.Metadata_ESSENTIAL,
	Category:    metric.Metadata_EXPIRATIONS,
	HowToUse:    "See Description.",
}

// GetLicenseTTL is a function which returns the TTL for the active cluster.
// The implementation here returns 0, but if utilccl is started this function is
// overridden with an appropriate getter.
var GetLicenseTTL = func(
	ctx context.Context,
	st *cluster.Settings,
	ts timeutil.TimeSource,
) int64 {
	return 0
}

// LicenseType returns what type of license the cluster is running with, or
// "OSS" if it is an OSS build.
//
// This function is overridden by an init hook in CCL builds.
var LicenseType = func(st *cluster.Settings) (string, error) {
	return "OSS", nil
}
