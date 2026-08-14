// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ccl

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
// This test lives in pkg/ccl, which blank-imports every CCL package, so the
// full settings registry (including the CCL-registered auth settings that
// pkg/cli does not transitively link on this release branch) is linked in.
// The pkg/cli sibling test covers the non-CCL subset; keep the two in sync.
func TestSensitiveSettingsRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expected := []string{
		"cloudstorage.http.custom_ca",
		"cluster.secret",
		"enterprise.license",
		"server.host_based_authentication.configuration",
		"server.identity_map.configuration",
		"server.jwt_authentication.issuers.custom_ca",
		"server.ldap_authentication.client.tls_certificate",
		"server.ldap_authentication.client.tls_key",
		"server.ldap_authentication.domain.custom_ca",
		"server.oidc_authentication.client_id",
		"server.oidc_authentication.client_secret",
		"server.oidc_authentication.provider.custom_ca",
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
