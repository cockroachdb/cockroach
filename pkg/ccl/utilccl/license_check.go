// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var enterpriseLicense = settings.RegisterStringSetting(
	settings.TenantWritable,
	"enterprise.license",
	"the encoded cluster license",
	"",
	settings.WithValidateString(
		func(sv *settings.Values, s string) error {
			_, err := decode(s)
			return err
		},
	),
	// Even though string settings are non-reportable by default, we
	// still mark them explicitly in case a future code change flips the
	// default.
	settings.WithReportable(false),
	settings.WithPublic,
)

// enterpriseStatus determines whether the cluster is enabled
// for enterprise features or if enterprise status depends on the license.
var enterpriseStatus int32 = deferToLicense

const (
	deferToLicense    = 0
	enterpriseEnabled = 1
)

// errEnterpriseRequired is returned by check() when the caller does
// not request detailed errors.
var errEnterpriseRequired = pgerror.New(pgcode.CCLValidLicenseRequired,
	"a valid enterprise license is required")

// licenseCacheKey is used to cache licenses in cluster.Settings.Cache,
// keeping the entries private.
type licenseCacheKey string

// TestingEnableEnterprise allows overriding the license check in tests.
//
// Prefer using ccl.TestingEnableEnterprise instead when that's possible without
// introducing a dependency cycle. This function is useful for packages that
// can't import `ccl` without introducing a dependency cycle. However, it panics
// if package `ccl` (which, in turn, imports all the other ccl/... packages)
// wasn't linked into the binary. In tests, generally the right thing to do is
// to import `ccl` from a file in the `foo_test` pkg (for example,
// main_test.go). Frequently, calling ccl.TestingEnableEnterprise in TestMain is
// used to enable the license for the tests in a whole package, so this
// utilccl.TestingEnableEnterprise is not frequently used. Even if this function
// didn't panic, CheckEnterpriseEnabled will panic if not all the CCL code is
// linked in, so callers of this function would likely just have the panic
// kicked down the road to the check call.
func TestingEnableEnterprise() func() {
	if !AllCCLCodeImported {
		panic("not all ccl code imported")
	}
	before := atomic.LoadInt32(&enterpriseStatus)
	atomic.StoreInt32(&enterpriseStatus, enterpriseEnabled)
	return func() {
		atomic.StoreInt32(&enterpriseStatus, before)
	}
}

// TestingDisableEnterprise allows re-enabling the license check in tests.
func TestingDisableEnterprise() func() {
	before := atomic.LoadInt32(&enterpriseStatus)
	atomic.StoreInt32(&enterpriseStatus, deferToLicense)
	return func() {
		atomic.StoreInt32(&enterpriseStatus, before)
	}
}

// ApplyTenantLicense verifies the COCKROACH_TENANT_LICENSE environment variable
// and enables enterprise features for the process. This is a bit of a hack and
// should be replaced once it is possible to read the host cluster's
// enterprise.license setting.
func ApplyTenantLicense() error {
	license, ok := envutil.EnvString("COCKROACH_TENANT_LICENSE", 0)
	if !ok {
		return nil
	}
	if _, err := decode(license); err != nil {
		return errors.Wrap(err, "COCKROACH_TENANT_LICENSE encoding is invalid")
	}
	atomic.StoreInt32(&enterpriseStatus, enterpriseEnabled)
	return nil
}

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
//
// This should not be used in hot paths, since an unavailable feature will
// result in a new error being instantiated for every call -- use
// IsEnterpriseEnabled() instead.
//
// The ClusterID argument should be the tenant-specific logical
// cluster ID.
// `feature` is not used for the check itself; it is merely embedded
// in the URL displayed in the error message.
func CheckEnterpriseEnabled(st *cluster.Settings, cluster uuid.UUID, feature string) error {
	return checkEnterpriseEnabledAt(st, timeutil.Now(), cluster, feature, true /* withDetails */)
}

// IsEnterpriseEnabled returns whether the requested enterprise feature is
// enabled. It is faster than CheckEnterpriseEnabled, since it does not return
// details about why the feature is unavailable, and can therefore be used in
// hot paths.
//
// The ClusterID argument should be the tenant-specific logical
// cluster ID.
// `feature` is not used for the check itself; it is merely embedded
// in the URL displayed in the error message.
func IsEnterpriseEnabled(st *cluster.Settings, cluster uuid.UUID, feature string) bool {
	return checkEnterpriseEnabledAt(
		st, timeutil.Now(), cluster, feature, false /* withDetails */) == nil
}

var licenseMetricUpdateFrequency = 1 * time.Minute

// UpdateMetricOnLicenseChange starts a task to periodically update
// the given metric with the seconds remaining until license expiry.
func UpdateMetricOnLicenseChange(
	ctx context.Context,
	st *cluster.Settings,
	metric *metric.Gauge,
	ts timeutil.TimeSource,
	stopper *stop.Stopper,
) error {
	enterpriseLicense.SetOnChange(&st.SV, func(ctx context.Context) {
		updateMetricWithLicenseTTL(ctx, st, metric, ts)
	})
	return stopper.RunAsyncTask(ctx, "write-license-expiry-metric", func(ctx context.Context) {
		ticker := time.NewTicker(licenseMetricUpdateFrequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				updateMetricWithLicenseTTL(ctx, st, metric, ts)
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

func updateMetricWithLicenseTTL(
	ctx context.Context, st *cluster.Settings, metric *metric.Gauge, ts timeutil.TimeSource,
) {
	license, err := getLicense(st)
	if err != nil {
		log.Errorf(ctx, "unable to update license expiry metric: %v", err)
		metric.Update(0)
		return
	}
	if license == nil {
		metric.Update(0)
		return
	}
	sec := timeutil.Unix(license.ValidUntilUnixSec, 0).Sub(ts.Now()).Seconds()
	metric.Update(int64(sec))
}

// AllCCLCodeImported is set by the `ccl` pkg in an init(), thereby
// demonstrating that we're in a binary that has all off the CCL code linked in.
var AllCCLCodeImported = false

func checkEnterpriseEnabledAt(
	st *cluster.Settings, at time.Time, cluster uuid.UUID, feature string, withDetails bool,
) error {
	if atomic.LoadInt32(&enterpriseStatus) == enterpriseEnabled {
		return nil
	}
	license, err := getLicense(st)
	if err != nil {
		return err
	}
	org := sql.ClusterOrganization.Get(&st.SV)
	if err := check(license, at, cluster, org, feature, withDetails); err != nil {
		return err
	}

	// Make sure all CCL code was imported.
	if !AllCCLCodeImported {
		// In production, this shouldn't happen: a binary that has `utilccl` linked
		// in will also have `ccl` linked in - which imports all the other ccl/...
		// packages and sets AllCCLCodeImported. However, in tests, one can write a
		// test that imports `utilccl`, without the broader `ccl`. In that case, we
		// panic here, forcing the test to link `ccl` in the binary (for example,
		// through importing `ccl` in the `foo_test` pkg, perhaps in
		// `main_test.go`). Declaring that "enterprise is enabled" when some CCL
		// functionality is not linked in would be very confusing - the server code
		// can try to use CCL features that are not available.
		//
		// Since this function cannot be used if `ccl` was not imported, the
		// function should arguably live in the `ccl` pkg. However, `ccl` imports
		// everything, and this function must be used from different packages that,
		// thus, cannot depend on `ccl`. AllCCLCodeImported is used to break the
		// dependency cycle.
		panic("not all ccl imported, but license checked")
	}
	return nil
}

// getLicense fetches the license from the given settings, using Settings.Cache
// to cache the decoded license (if any). The returned license must not be
// modified by the caller.
func getLicense(st *cluster.Settings) (*licenseccl.License, error) {
	str := enterpriseLicense.Get(&st.SV)
	if str == "" {
		return nil, nil
	}
	cacheKey := licenseCacheKey(str)
	if cachedLicense, ok := st.Cache.Load(cacheKey); ok {
		return cachedLicense.(*licenseccl.License), nil
	}
	license, err := decode(str)
	if err != nil {
		return nil, err
	}
	st.Cache.Store(cacheKey, license)
	return license, nil
}

func GetLicenseType(st *cluster.Settings) (string, error) {
	license, err := getLicense(st)
	if err != nil {
		return "", err
	} else if license == nil {
		return "None", nil
	}
	return license.Type.String(), nil
}

// decode attempts to read a base64 encoded License.
func decode(s string) (*licenseccl.License, error) {
	lic, err := licenseccl.Decode(s)
	if err != nil {
		return nil, pgerror.WithCandidateCode(err, pgcode.Syntax)
	}
	return lic, nil
}

// check returns an error if the license is empty or not currently valid. If
// withDetails is false, a generic error message is returned for performance.
func check(
	l *licenseccl.License, at time.Time, cluster uuid.UUID, org, feature string, withDetails bool,
) error {
	if l == nil {
		if !withDetails {
			return errEnterpriseRequired
		}
		// TODO(dt): link to some stable URL that then redirects to a helpful page
		// that explains what to do here.
		link := "https://cockroachlabs.com/pricing?cluster="
		return pgerror.Newf(pgcode.CCLValidLicenseRequired,
			"use of %s requires an enterprise license. "+
				"see %s%s for details on how to enable enterprise features",
			errors.Safe(feature),
			link,
			cluster.String(),
		)
	}

	// We extend some grace period to enterprise license holders rather than
	// suddenly throwing errors at them.
	if l.ValidUntilUnixSec > 0 && l.Type != licenseccl.License_Enterprise {
		if expiration := timeutil.Unix(l.ValidUntilUnixSec, 0); at.After(expiration) {
			if !withDetails {
				return errEnterpriseRequired
			}
			licensePrefix := redact.SafeString("")
			switch l.Type {
			case licenseccl.License_NonCommercial:
				licensePrefix = "non-commercial "
			case licenseccl.License_Evaluation:
				licensePrefix = "evaluation "
			}
			return pgerror.Newf(pgcode.CCLValidLicenseRequired,
				"Use of %s requires an enterprise license. Your %slicense expired on %s. If you're "+
					"interested in getting a new license, please contact subscriptions@cockroachlabs.com "+
					"and we can help you out.",
				errors.Safe(feature),
				licensePrefix,
				expiration.Format("January 2, 2006"),
			)
		}
	}

	if l.ClusterID == nil {
		if strings.EqualFold(l.OrganizationName, org) {
			return nil
		}
		if !withDetails {
			return errEnterpriseRequired
		}
		return pgerror.Newf(pgcode.CCLValidLicenseRequired,
			"license valid only for %q", l.OrganizationName)
	}

	for _, c := range l.ClusterID {
		if cluster == c {
			return nil
		}
	}

	// no match, so compose an error message.
	if !withDetails {
		return errEnterpriseRequired
	}
	var matches bytes.Buffer
	for i, c := range l.ClusterID {
		if i > 0 {
			matches.WriteString(", ")
		}
		matches.WriteString(c.String())
	}
	return pgerror.Newf(pgcode.CCLValidLicenseRequired,
		"license for cluster(s) %s is not valid for cluster %s",
		matches.String(), cluster.String(),
	)
}
