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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var enterpriseLicense = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		"enterprise.license",
		"the encoded cluster license",
		"",
		func(sv *settings.Values, s string) error {
			_, err := decode(s)
			return err
		},
	)
	// Even though string settings are non-reportable by default, we
	// still mark them explicitly in case a future code change flips the
	// default.
	s.SetReportable(false)
	s.SetVisibility(settings.Public)
	return s
}()

var testingEnterpriseEnabled = false

// TestingEnableEnterprise allows overriding the license check in tests.
func TestingEnableEnterprise() func() {
	before := testingEnterpriseEnabled
	testingEnterpriseEnabled = true
	return func() {
		testingEnterpriseEnabled = before
	}
}

// TestingDisableEnterprise allows re-enabling the license check in tests.
func TestingDisableEnterprise() func() {
	before := testingEnterpriseEnabled
	testingEnterpriseEnabled = false
	return func() {
		testingEnterpriseEnabled = before
	}
}

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
func CheckEnterpriseEnabled(st *cluster.Settings, cluster uuid.UUID, org, feature string) error {
	if testingEnterpriseEnabled {
		return nil
	}
	return checkEnterpriseEnabledAt(st, timeutil.Now(), cluster, org, feature)
}

func init() {
	base.CheckEnterpriseEnabled = CheckEnterpriseEnabled
	base.LicenseType = getLicenseType
	base.TimeToEnterpriseLicenseExpiry = TimeToEnterpriseLicenseExpiry
}

// TimeToEnterpriseLicenseExpiry returns a Duration from `asOf` until the current
// enterprise license expires. If a license does not exist, we return a
// zero duration.
func TimeToEnterpriseLicenseExpiry(
	ctx context.Context, st *cluster.Settings, asOf time.Time,
) (time.Duration, error) {
	var lic *licenseccl.License
	// FIXME(tschottdorf): see whether it makes sense to cache the decoded
	// license.
	if str := enterpriseLicense.Get(&st.SV); str != "" {
		var err error
		if lic, err = decode(str); err != nil {
			return 0, err
		}
	} else {
		return 0, nil
	}

	expiration := timeutil.Unix(lic.ValidUntilUnixSec, 0)
	return expiration.Sub(asOf), nil
}

func checkEnterpriseEnabledAt(
	st *cluster.Settings, at time.Time, cluster uuid.UUID, org, feature string,
) error {
	var lic *licenseccl.License
	// FIXME(tschottdorf): see whether it makes sense to cache the decoded
	// license.
	if str := enterpriseLicense.Get(&st.SV); str != "" {
		var err error
		if lic, err = decode(str); err != nil {
			return err
		}
	}
	return check(lic, at, cluster, org, feature)
}

func getLicenseType(st *cluster.Settings) (string, error) {
	str := enterpriseLicense.Get(&st.SV)
	if str == "" {
		return "None", nil
	}
	lic, err := decode(str)
	if err != nil {
		return "", err
	}
	return lic.Type.String(), nil
}

// decode attempts to read a base64 encoded License.
func decode(s string) (*licenseccl.License, error) {
	lic, err := licenseccl.Decode(s)
	if err != nil {
		return nil, pgerror.WithCandidateCode(err, pgcode.Syntax)
	}
	return lic, err
}

// check returns an error if the license is empty or not currently valid.
func check(l *licenseccl.License, at time.Time, cluster uuid.UUID, org, feature string) error {
	if l == nil {
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
		return pgerror.Newf(pgcode.CCLValidLicenseRequired,
			"license valid only for %q", l.OrganizationName)
	}

	for _, c := range l.ClusterID {
		if cluster == c {
			return nil
		}
	}

	// no match, so compose an error message.
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
