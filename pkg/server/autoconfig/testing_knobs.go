// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package autoconfig

import "github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"

type TestingKnobs struct {
	Provider acprovider.Provider
}

func (*TestingKnobs) ModuleTestingKnobs() {}
