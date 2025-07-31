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
	"github.com/cockroachdb/errors"
)

var errEnterpriseNotEnabled = errors.New("OSS binaries do not include enterprise features")

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
//
// This function is overridden by an init hook in CCL builds.
var CheckEnterpriseEnabled = func(_ *cluster.Settings, feature string) error {
	return errEnterpriseNotEnabled // nb: this is squarely in the hot path on OSS builds
}

// CCLDistributionAndEnterpriseEnabled is a simpler version of
// CheckEnterpriseEnabled which doesn't take in feature-related info and doesn't
// return an error with a nice message.
var CCLDistributionAndEnterpriseEnabled = func(st *cluster.Settings) bool {
	return CheckEnterpriseEnabled(st, "" /* feature */) == nil
}

var LicenseTTLMetadata = metric.Metadata{
	// This metric name isn't namespaced for backwards
	// compatibility. The prior version of this metric was manually
	// inserted into the prometheus output
	Name:        "seconds_until_enterprise_license_expiry",
	Help:        "Seconds until license expiry (0 if no license present)",
	Measurement: "Seconds",
	Unit:        metric.Unit_SECONDS,
}

var AdditionalLicenseTTLMetadata = metric.Metadata{
	Name:        "seconds_until_license_expiry",
	Help:        "Seconds until license expiry (0 if no license present)",
	Measurement: "Seconds",
	Unit:        metric.Unit_SECONDS,
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
