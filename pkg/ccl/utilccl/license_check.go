// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package utilccl

import (
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/pkg/errors"
)

// enterpriseEnabled indicates if the cluster has enterprise features enabled.
// TODO(dt): this is a stub for now, to be replaced with some sort of runtime
// configurable license key or other control mechanism.
var enterpriseEnabled = envutil.EnvOrDefaultBool("COCKROACH_ENTERPRISE_ENABLED", false)

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
func CheckEnterpriseEnabled(feature string) error {
	if enterpriseEnabled {
		return nil
	}
	// TODO(dt): link to some stable URL that then redirects to a helpful page
	// that explains what to do here.
	link := "https://cockroachlabs.com/pricing"
	return errors.Errorf(
		"use of %s requires an enterprise license. "+
			"see %s for details on how to enable enterprise features.",
		feature,
		link,
	)
}

// TestingEnableEnterprise overrides enterprise feature gating for testing.
func TestingEnableEnterprise(enabled bool) {
	enterpriseEnabled = enabled
}
