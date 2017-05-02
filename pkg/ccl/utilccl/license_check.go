// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package utilccl

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/pkg/errors"
)

// EnterpriseEnabled is temporary, until #14114 is implemented.
var EnterpriseEnabled = func() *settings.BoolSetting {
	name := "enterprise.enabled"
	s := settings.RegisterBoolSetting(name, "set to true to enable Enterprise features", false)
	settings.Hide(name)
	return s
}()

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
func CheckEnterpriseEnabled(feature string) error {
	if EnterpriseEnabled.Get() {
		return nil
	}
	// TODO(dt): link to some stable URL that then redirects to a helpful page
	// that explains what to do here.
	link := "https://cockroachlabs.com/pricing"
	return errors.Errorf(
		"use of %s requires an enterprise license. "+
			"see %s for details on how to enable enterprise features",
		feature,
		link,
	)
}
