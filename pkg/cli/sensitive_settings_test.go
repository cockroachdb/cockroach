// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestSensitiveSettingsRegistry locks down the set of Sensitive-marked
// cluster settings. The Sensitive marker means "the value is a secret": it
// suppresses the value from statement bundles, debug zips, and the settings
// APIs, even for privileged collectors. Update this list only for settings
// that actually hold secrets: marking anything else needlessly destroys
// diagnostic information, while failing to mark a secret-holding setting
// leaks its value into artifacts that are shared with Cockroach Labs.
//
// This test only covers the settings that pkg/cli transitively links in.
// On this release branch that is the non-CCL subset; the CCL-registered
// auth settings (jwt/ldap/oidc) are not linked here, so the full registry
// is locked down by the sibling test in pkg/ccl, which blank-imports every
// CCL package. Keep the two lists in sync.
func TestSensitiveSettingsRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expected := []string{
		"cloudstorage.http.custom_ca",
		"cluster.secret",
		"enterprise.license",
	}
	var actual []string
	for _, k := range settings.Keys(true /* forSystemTenant */) {
		s, ok := settings.LookupForLocalAccessByKey(k, true /* forSystemTenant */)
		if ok && s.IsSensitive() {
			actual = append(actual, string(s.Name()))
		}
	}
	require.ElementsMatch(t, expected, actual)
}
