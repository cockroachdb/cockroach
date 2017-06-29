// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package utilccl

import (
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// EnterpriseEnabled is temporary, until #14114 is implemented.
var EnterpriseEnabled = func() *settings.BoolSetting {
	s := settings.RegisterBoolSetting("enterprise.enabled", "set to true to enable Enterprise features", false)
	s.Hide()
	return s
}()

var currentLicense = func() *atomic.Value {
	setting := settings.RegisterValidatedStringSetting(
		"enterprise.license", "the encoded cluster license", "",
		func(s string) error {
			_, err := licenseccl.Decode(s)
			return err
		})
	setting.Hide()
	var ref atomic.Value
	setting.OnChange(func() {
		cur, err := licenseccl.Decode(setting.Get())
		if err != nil {
			log.Warningf(context.Background(), "error decoding license: %v", err)
		} else {
			ref.Store(cur)
		}
	})
	return &ref
}()

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
func CheckEnterpriseEnabled(cluster uuid.UUID, org, feature string) error {
	// TODO(dt): delete this after a transition period, before 1.1.
	if EnterpriseEnabled.Get() {
		return nil
	}
	return checkEnterpriseEnabledAt(timeutil.Now(), cluster, org, feature)
}

func checkEnterpriseEnabledAt(at time.Time, cluster uuid.UUID, org, feature string) error {
	var lic *licenseccl.License
	if licPtr := currentLicense.Load(); licPtr != nil {
		lic = licPtr.(*licenseccl.License)
	}
	return lic.Check(at, cluster, org, feature)
}
