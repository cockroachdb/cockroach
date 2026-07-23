// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package e2e_tests

import "embed"

//go:embed cypress.config.ts
var cypressConfig embed.FS

//go:embed package.json
var packageJson embed.FS

//go:embed tsconfig.json
var tsConfigJson embed.FS

//go:embed cypress/e2e
var cypressE2E embed.FS

//go:embed cypress/fixtures
var cypressFixtures embed.FS

//go:embed cypress/support
var cypressSupport embed.FS

// CypressEmbeds contains all the embed.FS files needed to run e2e cypress tests
var CypressEmbeds = []embed.FS{cypressConfig, packageJson, tsConfigJson, cypressE2E, cypressFixtures, cypressSupport}
