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
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var errEnterpriseNotEnabled = errors.New("OSS binaries do not include enterprise features")

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
//
// This function is overridden by an init hook in CCL builds.
var CheckEnterpriseEnabled = func(_ *cluster.Settings, _ uuid.UUID, org, feature string) error {
	return errEnterpriseNotEnabled // nb: this is squarely in the hot path on OSS builds
}

// TimeToEnterpriseLicenseExpiry returns a duration object that measures the time until
// the currently set enterprise license expires starting from the 3rd argument
// passed in.
//
// This function is overridden by an init hook in CCL builds
var TimeToEnterpriseLicenseExpiry = func(
	ctx context.Context, _ *cluster.Settings, _ time.Time,
) (time.Duration, error) {
	return 0, nil
}

// LicenseType returns what type of license the cluster is running with, or
// "OSS" if it is an OSS build.
//
// This function is overridden by an init hook in CCL builds.
var LicenseType = func(st *cluster.Settings) (string, error) {
	return "OSS", nil
}
