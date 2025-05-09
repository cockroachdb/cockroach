// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package fipsccl

import "github.com/cockroachdb/cockroach/pkg/ccl/securityccl/fipsccl/fipscclbase"

// IsCompileTimeFIPSReady is an alias for the function of the same name in
// fipscclbase.
func IsCompileTimeFIPSReady() bool {
	return fipscclbase.IsCompileTimeFIPSReady()
}

// IsOpenSSLLoaded is an alias for the function of the same name in fipscclbase.
func IsOpenSSLLoaded() bool {
	return fipscclbase.IsOpenSSLLoaded()
}

// IsFIPSReady is an alias for the function of the same name in fipscclbase.
func IsFIPSReady() bool {
	return fipscclbase.IsFIPSReady()
}

// BuildOpenSSLVersion is an alias for the function of the same name in
// fipscclbase.
func BuildOpenSSLVersion() (string, string, error) {
	return fipscclbase.BuildOpenSSLVersion()
}
