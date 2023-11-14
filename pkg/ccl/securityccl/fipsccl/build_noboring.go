// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
//
//go:build !boringcrypto

package fipsccl

import "github.com/cockroachdb/errors"

func IsCompileTimeFIPSReady() bool {
	return false
}

func IsOpenSSLLoaded() bool {
	return false
}

func IsFIPSReady() bool {
	return false
}

func BuildOpenSSLVersion() (string, string, error) {
	return "", "", errors.New("openssl support not present")
}
