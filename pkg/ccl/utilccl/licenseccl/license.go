// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package licenseccl

import (
	"bytes"
	"encoding/base64"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// LicensePrefix is a prefix on license strings to make them easily recognized.
const LicensePrefix = "crl-0-"

// Encode serializes the License as a base64 string.
func (l License) Encode() (string, error) {
	bytes, err := protoutil.Marshal(&l)
	if err != nil {
		return "", err
	}
	return LicensePrefix + base64.RawStdEncoding.EncodeToString(bytes), nil
}

// Decode attempts to read a base64 encoded License.
func Decode(s string) (*License, error) {
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
	if err := protoutil.Unmarshal(data, &lic); err != nil {
		return nil, errors.Wrap(err, "invalid license string")
	}
	return &lic, nil
}

// Check returns an error if the license is empty or not currently valid.
func (l *License) Check(at time.Time, cluster uuid.UUID, org, feature string) error {
	if l == nil {
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

	// We extend some grace period to enterprise license holders rather than
	// suddenly throwing errors at them.
	if l.ValidUntilUnixSec > 0 && l.Type != License_Enterprise {
		if expiration := timeutil.Unix(l.ValidUntilUnixSec, 0); at.After(expiration) {
			return errors.Errorf("license expired at %s", expiration.String())
		}
	}

	if l.ClusterID == nil {
		if strings.EqualFold(l.OrganizationName, org) {
			return nil
		}
		return errors.Errorf("license valid only for %q", l.OrganizationName)
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
	return errors.Errorf(
		"license for cluster(s) %s is not valid for cluster %s", matches.String(), cluster.String(),
	)
}
