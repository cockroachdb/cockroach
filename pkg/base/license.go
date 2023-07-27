// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var errEnterpriseNotEnabled = errors.New("OSS binaries do not include enterprise features")

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
//
// This function is overridden by an init hook in CCL builds.
var CheckEnterpriseEnabled = func(_ *cluster.Settings, _ uuid.UUID, feature string) error {
	return errEnterpriseNotEnabled // nb: this is squarely in the hot path on OSS builds
}

// CCLDistributionAndEnterpriseEnabled is a simpler version of
// CheckEnterpriseEnabled which doesn't take in feature-related info and doesn't
// return an error with a nice message.
var CCLDistributionAndEnterpriseEnabled = func(st *cluster.Settings, clusterID uuid.UUID) bool {
	return CheckEnterpriseEnabled(st, clusterID, "" /* feature */) == nil
}

var licenseTTLMetadata = metric.Metadata{
	// This metric name isn't namespaced for backwards
	// compatibility. The prior version of this metric was manually
	// inserted into the prometheus output
	Name:        "seconds_until_enterprise_license_expiry",
	Help:        "Seconds until enterprise license expiry (0 if no license present or running without enterprise features)",
	Measurement: "Seconds",
	Unit:        metric.Unit_SECONDS,
}

// LicenseTTL is a metric gauge that measures the number of seconds
// until the current enterprise license (if any) expires.
var LicenseTTL = metric.NewGauge(licenseTTLMetadata)

// UpdateMetricOnLicenseChange is a function that's called on startup
// in order to connect the enterprise license setting update to the
// prometheus metric provided as an argument.
var UpdateMetricOnLicenseChange = func(
	ctx context.Context,
	st *cluster.Settings,
	metric *metric.Gauge,
	ts timeutil.TimeSource,
	stopper *stop.Stopper,
) error {
	return nil
}

// LicenseType returns what type of license the cluster is running with, or
// "OSS" if it is an OSS build.
//
// This function is overridden by an init hook in CCL builds.
var LicenseType = func(st *cluster.Settings) (string, error) {
	return "OSS", nil
}
