// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
//go:build boringcrypto

package fipsccl

/*
#include <openssl/ossl_typ.h>

static unsigned long _fipsccl_openssl_version_number() {
	return OPENSSL_VERSION_NUMBER;
}
*/
import "C"

import (
	"crypto/boring"
	"fmt"
)

// IsCompileTimeFIPSReady returns true if this binary was built with correct
// toolchain and options, which is a prerequisite for FIPS-ready mode.
// Note that we only support the golang-fips toolchain even though the
// build tag we test for is "boringcrypto". The two are not actually
// compatible because crypto/boring.Enabled is a bool in one and a function
// in the other.
func IsCompileTimeFIPSReady() bool {
	return true
}

// IsOpenSSLLoaded returns true if the OpenSSL library has been found and
// loaded.
func IsOpenSSLLoaded() bool {
	return boring.Enabled()
}

// IsFIPSReady returns true if all of our FIPS readiness checks succeed.
func IsFIPSReady() bool {
	// The golang-fips toolchain only attempts to load OpenSSL if the kernel
	// fips mode is enabled. Therefore we only need this single check for our
	// overall fips-readiness status. We could redundantly call IsBoringBuild
	// and IsKernelEnabled, but doing so would risk some divergence between our
	// implementation and the toolchain itself so it's better at this time to
	// use the single check.
	return IsOpenSSLLoaded()
}

// BuildOpenSSLVersion returns the version number of OpenSSL that was used at
// build time. The first return value is the hex value of the
// OPENSSL_VERSION_NUMBER constant (for example, 10100000 for OpenSSL 1.1 and
// 30000000 for OpenSSL 3.0), and the second is the versioned name of the
// libcrypto.so file.
func BuildOpenSSLVersion() (string, string, error) {
	buildVersion := uint64(C._fipsccl_openssl_version_number())
	var soname string
	// Reference:
	// https://github.com/golang-fips/go/blob/7f64529ab80e5d394bb2496e982d6f6e11023902/patches/001-initial-openssl-for-fips.patch#L3476-L3482
	if buildVersion < 0x10100000 {
		soname = "libcrypto.so.10"
	} else if buildVersion < 0x30000000 {
		soname = "libcrypto.so.1.1"
	} else {
		soname = "libcrypto.so.3"
	}
	return fmt.Sprintf("%x", buildVersion), soname, nil
}
