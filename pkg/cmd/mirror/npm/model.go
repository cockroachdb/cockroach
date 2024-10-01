// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

type PnpmLockfile struct {
	Version  string                 `yaml:"version"`
	Packages map[string]interface{} `yaml:"packages"`
	// Other fields exist in the lockfile but are not needed.
}
