// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package utilccl

import (
	"bytes"
	"encoding/base64"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// LicensePrefix is a prefix on license strings to make them easily recognized.
const LicensePrefix = "crl-0-"

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
			_, err := decodeLicense(s)
			return err
		})
	setting.Hide()
	var ref atomic.Value
	setting.OnChange(func() {
		cur, err := decodeLicense(setting.Get())
		if err != nil {
			log.Warningf(context.Background(), "error decoding license: %v", err)
		} else {
			ref.Store(cur)
		}
	})
	return &ref
}()

func decodeLicense(s string) (*License, error) {
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, LicensePrefix) {
		return nil, errors.New("invalid license string")
	}
	s = strings.TrimPrefix(s, LicensePrefix)
	data, err := base64.RawStdEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.Wrap(err, "invalid license string")
	}
	var lic License
	if err := lic.Unmarshal(data); err != nil {
		return nil, errors.Wrap(err, "invalid license string")
	}
	return &lic, nil
}

// CheckEnterpriseEnabled returns a non-nil error if the requested enterprise
// feature is not enabled, including information or a link explaining how to
// enable it.
func CheckEnterpriseEnabled(cluster uuid.UUID, feature string) error {
	// TODO(dt): delete this after a transition period, before 1.1.
	if EnterpriseEnabled.Get() {
		return nil
	}
	return checkEnterpriseEnabledAt(cluster, timeutil.Now(), feature)
}

func checkEnterpriseEnabledAt(cluster uuid.UUID, at time.Time, feature string) error {
	if licPtr := currentLicense.Load(); licPtr != nil {
		lic := licPtr.(*License)
		if lic != nil {
			if err := lic.checkCluster(cluster); err != nil {
				return err
			}
			return lic.checkExpiration(at)
		}
	}

	// TODO(dt): link to some stable URL that then redirects to a helpful page
	// that explains what to do here.
	link := "https://cockroachlabs.com/pricing?cluster="
	return errors.Errorf(
		"use of %s requires an enterprise license. "+
			"see %s%s for details on how to enable enterprise features",
		feature,
		link,
		cluster.String(),
	)
}

func (l License) checkCluster(cluster uuid.UUID) error {
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
	return errors.Errorf(
		"license for cluster(s) %s is not valid for cluster %s", matches.String(), cluster.String(),
	)
}

// checkExpiration returns an error if the license is expired.
func (l License) checkExpiration(at time.Time) error {
	if expiration := l.ValidUntil(); at.After(expiration) {
		// We extend some grace period to enterprise license holders rather than
		// suddenly throwing errors at them.
		if l.Type != License_Enterprise {
			return errors.Errorf("license expired at %s", expiration.String())
		}
	}
	return nil
}

// ValidUntil wraps the license expiration time in a time.Time.
func (l License) ValidUntil() time.Time {
	return time.Unix(l.ValidUntilUnixSec, 0)
}

// Encode serializes the license as a string.
func (l License) Encode() (string, error) {
	bytes, err := l.Marshal()
	if err != nil {
		return "", err
	}
	return LicensePrefix + base64.RawStdEncoding.EncodeToString(bytes), nil
}
