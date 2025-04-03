// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build crdb_test && !crdb_test_off

// Package buildutil provides a constant CrdbTestBuild.
package buildutil

// CrdbTestBuild is a flag that is set to true if the binary was compiled
// with the 'crdb_test' build tag (which is the case for all test targets). This
// flag can be used to enable expensive checks, test randomizations, or other
// metamorphic-style perturbations that will not affect test results but will
// exercise different parts of the code.
const CrdbTestBuild = true
