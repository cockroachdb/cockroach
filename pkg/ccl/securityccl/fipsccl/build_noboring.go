// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
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
