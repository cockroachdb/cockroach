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

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
