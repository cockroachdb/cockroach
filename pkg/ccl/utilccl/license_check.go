// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utilccl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	licenseserver "github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// trialLicenseExpiryTimestamp tracks the expiration timestamp of any trial licenses
// that have been installed on this cluster (past or present).
var trialLicenseExpiryTimestamp atomic.Int64

var EnterpriseLicense = settings.RegisterStringSetting(
	settings.SystemVisible,
	"enterprise.license",
	"the encoded cluster license",
	"",
	settings.WithValidateString(
		func(sv *settings.Values, s string) error {
			l, err := decode(s)
			if err != nil {
				return err
			}
			if l == nil {
				return nil
			}

			if l.Type == licenseccl.License_Trial && trialLicenseExpiryTimestamp.Load() > 0 &&
				l.ValidUntilUnixSec != trialLicenseExpiryTimestamp.Load() {
				return errors.WithHint(errors.Newf("a trial license has previously been installed on this cluster"),
					"Please install a non-trial license to continue")
			}

			return nil
		},
	),
	// Even though string settings are non-reportable by default, we
	// still mark them explicitly in case a future code change flips the
	// default.
	settings.WithReportable(false),
	settings.WithPublic,
)

// licenseCacheKey is used to cache licenses in cluster.Settings.Cache,
// keeping the entries private.
type licenseCacheKey string

// TestingEnableEnterprise allows overriding the license check in tests. This
// function was deprecated when the core license was removed. We no longer
// distinguish between features enabled only for enterprise. All features are
// enabled, and if a license policy is violated, we throttle connections.
// Callers can safely remove any reference to this function.
//
// Deprecated
func TestingEnableEnterprise() func() {
	return func() {}
}

// TestingDisableEnterprise allows re-enabling the license check in tests.
//
// See description in TestingEnableEnterprise for rationale about deprecation.
//
// Deprecated
func TestingDisableEnterprise() func() {
	return func() {}
}

// CheckEnterpriseEnabled previously returned a non-nil error if the requested enterprise
// feature was not enabled. It is now deprecated and always returns nil. Callers should
// remove any usage of this function.
//
// Deprecated
func CheckEnterpriseEnabled(*cluster.Settings, string) error {
	return nil
}

// IsEnterpriseEnabled previously returned whether the requested enterprise
// feature was enabled. It is now deprecated and always returns true. Callers
// should remove usage of this function.
//
// Deprecated
func IsEnterpriseEnabled(*cluster.Settings, string) bool {
	return true
}

// GetLicenseTTL is a function which returns the TTL for the active cluster.
// This is done by reading the license information from the cluster settings
// and subtracting the epoch from the expiry timestamp.
var GetLicenseTTL = func(
	ctx context.Context,
	st *cluster.Settings,
	ts timeutil.TimeSource,
) int64 {
	license, err := GetLicense(st)
	if err != nil {
		log.Errorf(ctx, "unable to find license: %v", err)
		return 0
	}
	if license == nil {
		return 0
	}
	sec := timeutil.Unix(license.ValidUntilUnixSec, 0).Sub(ts.Now()).Seconds()
	return int64(sec)
}

// GetLicense fetches the license from the given settings, using Settings.Cache
// to cache the decoded license (if any). The returned license must not be
// modified by the caller.
func GetLicense(st *cluster.Settings) (*licenseccl.License, error) {
	str := EnterpriseLicense.Get(&st.SV)
	if str == "" {
		return nil, nil
	}
	cacheKey := licenseCacheKey(str)
	if cachedLicense, ok := st.Cache.Load(cacheKey); ok {
		return (*cachedLicense).(*licenseccl.License), nil
	}
	license, err := decode(str)
	if err != nil {
		return nil, err
	}
	licenseBox := any(license)
	st.Cache.Store(cacheKey, &licenseBox)
	return license, nil
}

// GetLicenseType returns the license type.
func GetLicenseType(st *cluster.Settings) (string, error) {
	license, err := GetLicense(st)
	if err != nil {
		return "", err
	} else if license == nil {
		return "None", nil
	}
	return license.Type.String(), nil
}

// GetLicenseEnvironment returns the license environment.
func GetLicenseEnvironment(st *cluster.Settings) (string, error) {
	license, err := GetLicense(st)
	if err != nil {
		return "", err
	} else if license == nil {
		return "", nil
	}
	return license.Environment.String(), nil
}

// decode attempts to read a base64 encoded License.
func decode(s string) (*licenseccl.License, error) {
	lic, err := licenseccl.Decode(s)
	if err != nil {
		return nil, pgerror.WithCandidateCode(err, pgcode.Syntax)
	}
	return lic, nil
}

// RegisterCallbackOnLicenseChange will register a callback to update the
// license enforcer whenever the license changes.
func RegisterCallbackOnLicenseChange(
	ctx context.Context, st *cluster.Settings, licenseEnforcer *licenseserver.Enforcer,
) {
	// refreshFunc is the function responsible for refreshing the enforcer's state.
	// The isChange parameter indicates whether the license is actually being updated,
	// as opposed to merely refreshing the current license.
	refreshFunc := func(ctx context.Context, isChange bool) {
		lic, err := GetLicense(st)
		if err != nil {
			log.Errorf(ctx, "unable to refresh license enforcer for license change: %v", err)
			return
		}
		var licenseType licenseserver.LicType
		var licenseExpiry time.Time
		if lic == nil {
			licenseType = licenseserver.LicTypeNone
		} else {
			licenseExpiry = timeutil.Unix(lic.ValidUntilUnixSec, 0)
			switch lic.Type {
			case licenseccl.License_Free:
				licenseType = licenseserver.LicTypeFree
			case licenseccl.License_Trial:
				licenseType = licenseserver.LicTypeTrial
			case licenseccl.License_Evaluation:
				licenseType = licenseserver.LicTypeEvaluation
			default:
				licenseType = licenseserver.LicTypeEnterprise
			}
		}
		licenseEnforcer.RefreshForLicenseChange(ctx, licenseType, licenseExpiry)

		expiry, err := licenseEnforcer.UpdateTrialLicenseExpiry(ctx, licenseType, isChange, licenseExpiry.Unix())
		if err != nil {
			log.Errorf(ctx, "unable to update trial license expiry: %v", err)
			return
		}
		trialLicenseExpiryTimestamp.Store(expiry)
	}
	// Install the hook so that we refresh license details when the license changes.
	EnterpriseLicense.SetOnChange(&st.SV,
		func(ctx context.Context) { refreshFunc(ctx, true /* isChange */) })
	// Call the refresh function for the current license.
	refreshFunc(ctx, false /* isChange */)
}
