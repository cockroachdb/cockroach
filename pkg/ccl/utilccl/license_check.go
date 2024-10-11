// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utilccl

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	licenseserver "github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// trialLicenseExpiryTimestamp tracks the expiration timestamp of any trial licenses
// that have been installed on this cluster (past or present).
var trialLicenseExpiryTimestamp atomic.Int64

var enterpriseLicense = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		settings.TenantWritable,
		"enterprise.license",
		"the encoded cluster license",
		"",
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
	)
	// Even though string settings are non-reportable by default, we
	// still mark them explicitly in case a future code change flips the
	// default.
	s.SetReportable(false)
	s.SetVisibility(settings.Public)
	return s
}()

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

// TestingEnableEnterprise allows overriding the license check in tests. This
// function was deprecated when the core license was removed. We no longer
// distinguish between features enabled only for enterprise. All features are
// enabled, and if a license policy is violated, we throttle connections.
// Callers can safely remove any reference to this function.
//
// However, there is one exception to above mentioned. If a test is using
// TestingCheckEnterpriseEnabledLegacy, then this function maintains its
// original behaviour.
//
// Deprecated
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
//
// See description in TestingEnableEnterprise for rationale about deprecation.
//
// Deprecated
func TestingDisableEnterprise() func() {
	before := atomic.LoadInt32(&enterpriseStatus)
	atomic.StoreInt32(&enterpriseStatus, deferToLicense)
	return func() {
		atomic.StoreInt32(&enterpriseStatus, before)
	}
}

// CheckEnterpriseEnabled previously returned a non-nil error if the requested enterprise
// feature was not enabled. It is now deprecated and always returns nil. Callers should
// remove any usage of this function.
//
// Deprecated
func CheckEnterpriseEnabled(*cluster.Settings, uuid.UUID, string) error {
	return nil
}

// TestingCheckEnterpriseEnabledLegacy gives you the previous behaviour of
// CheckEnterpriseEnabled. It is intended to be used in tests only. And only for
// tests that relied on it to function properly.
func TestingCheckEnterpriseEnabledLegacy(
	st *cluster.Settings, cluster uuid.UUID, feature string,
) error {
	return checkEnterpriseEnabledAt(st, timeutil.Now(), cluster, feature, true /* withDetails */)
}

// IsEnterpriseEnabled previously returned whether the requested enterprise
// feature was enabled. It is now deprecated and always returns true. Callers
// should remove usage of this function.
//
// Deprecated
func IsEnterpriseEnabled(*cluster.Settings, uuid.UUID, string) bool {
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
	license, err := getLicense(st)
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

// RegisterCallbackOnLicenseChange will register a callback to update the
// license enforcer whenever the license changes.
func RegisterCallbackOnLicenseChange(
	ctx context.Context, st *cluster.Settings, licenseEnforcer *licenseserver.Enforcer,
) {
	// refreshFunc is the function responsible for refreshing the enforcer's state.
	// The isChange parameter indicates whether the license is actually being updated,
	// as opposed to merely refreshing the current license.
	refreshFunc := func(ctx context.Context, isChange bool) {
		lic, err := getLicense(st)
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
	enterpriseLicense.SetOnChange(&st.SV,
		func(ctx context.Context) { refreshFunc(ctx, true /* isChange */) })
	// Call the refresh function for the current license.
	refreshFunc(ctx, false /* isChange */)
}
