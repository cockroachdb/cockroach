// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/license/licensepb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// LicenseTTLMetadata is the metric metadata for seconds until license expiry.
var LicenseTTLMetadata = metric.Metadata{
	// This metric name isn't namespaced for backwards compatibility. The
	// prior version of this metric was manually inserted into the prometheus
	// output.
	Name:        "seconds_until_enterprise_license_expiry",
	Help:        "Seconds until license expiry (0 if no license present)",
	Measurement: "Seconds",
	Unit:        metric.Unit_SECONDS,
	Visibility:  metric.Metadata_ESSENTIAL,
	Category:    metric.Metadata_EXPIRATIONS,
	HowToUse:    "See Description.",
}

// AdditionalLicenseTTLMetadata is an additional metric for license TTL under
// a different metric name.
var AdditionalLicenseTTLMetadata = metric.Metadata{
	Name:        "seconds_until_license_expiry",
	Help:        "Seconds until license expiry (0 if no license present)",
	Measurement: "Seconds",
	Unit:        metric.Unit_SECONDS,
	Visibility:  metric.Metadata_ESSENTIAL,
	Category:    metric.Metadata_EXPIRATIONS,
	HowToUse:    "See Description.",
}

// trialLicenseExpiryTimestamp tracks the expiration timestamp of any trial
// licenses that have been installed on this cluster (past or present).
var trialLicenseExpiryTimestamp atomic.Int64

// EnterpriseLicense is the cluster setting that stores the encoded license.
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

			if l.Type == licensepb.License_Trial &&
				trialLicenseExpiryTimestamp.Load() > 0 &&
				l.ValidUntilUnixSec != trialLicenseExpiryTimestamp.Load() {
				return errors.WithHint(
					errors.Newf("a trial license has previously been installed on this cluster"),
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

// GetLicenseTTL returns the TTL for the active cluster license. It reads the
// license from the cluster settings and computes the remaining time until
// expiry.
func GetLicenseTTL(ctx context.Context, st *cluster.Settings, ts timeutil.TimeSource) int64 {
	license, err := GetLicense(st)
	if err != nil {
		log.Dev.Errorf(ctx, "unable to find license: %v", err)
		return 0
	}
	if license == nil {
		return 0
	}
	sec := timeutil.Unix(license.ValidUntilUnixSec, 0).Sub(ts.Now()).Seconds()
	return int64(sec)
}

// GetLicense fetches the license from the given settings, using
// Settings.Cache to cache the decoded license (if any). The returned license
// must not be modified by the caller.
func GetLicense(st *cluster.Settings) (*licensepb.License, error) {
	str := EnterpriseLicense.Get(&st.SV)
	if str == "" {
		return nil, nil
	}
	cacheKey := licenseCacheKey(str)
	if cachedLicense, ok := st.Cache.Load(cacheKey); ok {
		return (*cachedLicense).(*licensepb.License), nil
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
func decode(s string) (*licensepb.License, error) {
	lic, err := licensepb.Decode(s)
	if err != nil {
		return nil, pgerror.WithCandidateCode(err, pgcode.Syntax)
	}
	return lic, nil
}

// registerCallbackOnLicenseChange registers a callback to update the license
// enforcer whenever the license changes.
func registerCallbackOnLicenseChange(
	ctx context.Context, st *cluster.Settings, licenseEnforcer *Enforcer,
) {
	if st == nil {
		return
	}
	// refreshFunc is responsible for refreshing the enforcer's state. The
	// isChange parameter indicates whether the license is actually being
	// updated, as opposed to merely refreshing the current license.
	refreshFunc := func(ctx context.Context, isChange bool) {
		lic, err := GetLicense(st)
		if err != nil {
			log.Dev.Errorf(ctx,
				"unable to refresh license enforcer for license change: %v", err)
			return
		}
		var licenseType LicType
		var licenseExpiry time.Time
		if lic == nil {
			licenseType = LicTypeNone
		} else {
			licenseExpiry = timeutil.Unix(lic.ValidUntilUnixSec, 0)
			switch lic.Type {
			case licensepb.License_Free:
				licenseType = LicTypeFree
			case licensepb.License_Trial:
				licenseType = LicTypeTrial
			case licensepb.License_Evaluation:
				licenseType = LicTypeEvaluation
			default:
				licenseType = LicTypeEnterprise
			}
		}
		licenseEnforcer.RefreshForLicenseChange(ctx, licenseType, licenseExpiry)

		err = licenseEnforcer.UpdateTrialLicenseExpiry(
			ctx, licenseType, isChange, licenseExpiry.Unix())
		if err != nil {
			log.Dev.Errorf(ctx,
				"unable to update trial license expiry: %v", err)
			return
		}
	}
	// Install the hook so that we refresh license details when the license
	// changes.
	EnterpriseLicense.SetOnChange(&st.SV,
		func(ctx context.Context) { refreshFunc(ctx, true /* isChange */) })
	// Call the refresh function for the current license.
	refreshFunc(ctx, false /* isChange */)
}
