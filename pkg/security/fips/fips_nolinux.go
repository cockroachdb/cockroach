// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
//go:build !linux

package fips

import "github.com/cockroachdb/errors"

func IsKernelEnabled() (bool, error) {
	return false, errors.New("only supported on linux")
}
