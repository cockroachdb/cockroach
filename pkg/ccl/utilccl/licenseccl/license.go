// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package licenseccl

import (
	"encoding/base64"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// LicensePrefix is a prefix on license strings to make them easily recognized.
const LicensePrefix = "crl-0-"

// Encode serializes the License as a base64 string.
func (l *License) Encode() (string, error) {
	bytes, err := protoutil.Marshal(l)
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
		return nil, errors.Wrapf(err, "invalid license string")
	}
	var lic License
	if err := protoutil.Unmarshal(data, &lic); err != nil {
		return nil, errors.Wrap(err, "invalid license string")
	}
	return &lic, nil
}
