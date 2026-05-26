// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package identmap

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdentityMapElement(t *testing.T) {
	exactMatch := func(sysName, dbname string) element {
		return element{
			dbUser:       dbname,
			pattern:      regexp.MustCompile("(?i)^" + regexp.QuoteMeta(sysName) + "$"),
			substituteAt: -1,
		}
	}
	regexMatch := func(sysName, dbName string) element {
		return element{
			dbUser:       dbName,
			pattern:      regexp.MustCompile("(?i)" + sysName),
			substituteAt: strings.Index(dbName, `\1`),
		}
	}

	tcs := []struct {
		elt       element
		principal string
		expected  string
	}{
		{
			elt:       exactMatch("carlito", "carl"),
			principal: "carlito",
			expected:  "carl",
		},
		{
			elt:       exactMatch("carlito", "carl"),
			principal: "nope",
			expected:  "",
		},
		{
			elt:       regexMatch("^(.*)@cockroachlabs.com$", `\1`),
			principal: "carl@cockroachlabs.com",
			expected:  "carl",
		},
		{
			elt:       regexMatch("^(.*)@cockroachlabs.com$", `\11`),
			principal: "carl@cockroachlabs.com",
			expected:  "carl1",
		},
		{
			elt:       regexMatch("^(.*)@cockroachlabs.com$", `1\1`),
			principal: "carl@cockroachlabs.com",
			expected:  "1carl",
		},
		{
			elt:       regexMatch("^(.*)@cockroachlabs.com$", `\1`),
			principal: "carl@example.com",
			expected:  "",
		},
		// Case-insensitive exact match tests.
		{
			elt:       exactMatch("CaRlItO", "carl"),
			principal: "carlito",
			expected:  "carl",
		},
		{
			elt:       exactMatch("carlito", "carl"),
			principal: "CARLITO",
			expected:  "carl",
		},
		{
			elt:       exactMatch("CaRlItO", "carl"),
			principal: "CaRlItO",
			expected:  "carl",
		},
		// Case-insensitive regex match tests.
		{
			elt:       regexMatch("^(.*)@CockroachLabs.com$", `\1`),
			principal: "carl@cockroachlabs.com",
			expected:  "carl",
		},
		{
			elt:       regexMatch("^(.*)@cockroachlabs.com$", `\1`),
			principal: "Carl@CockroachLabs.COM",
			expected:  "Carl",
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			a.Equal(tc.expected, tc.elt.substitute(tc.principal))
		})
	}
}

func TestIdentityMap(t *testing.T) {
	a := assert.New(t)
	data := `
# This is a comment
map-name system-username              database-username
foo      /^(.*)@cockroachlabs.com$    \1
foo      carl@cockroachlabs.com       also_carl   # Trailing comment
foo      carl@cockroachlabs.com       carl        # Duplicate behavior
`

	m, err := From(strings.NewReader(data))
	if !a.NoError(err) {
		return
	}
	t.Log(m.String())
	a.Len(m.data, 2)

	a.Nil(m.Map("missing", "carl"))

	if elts := m.data["map-name"]; a.Len(elts, 1) {
		a.Equal("database-username", elts[0].substitute("system-username"))
	}

	if elts, _, err := m.Map("foo", "carl@cockroachlabs.com"); a.NoError(err) && a.Len(elts, 2) {
		a.Equal("carl", elts[0].Normalized())
		a.Equal("also_carl", elts[1].Normalized())
	}

	a.Nil(m.Map("foo", "carl@example.com"))
}

func TestIdentityMapExists(t *testing.T) {
	a := assert.New(t)
	data := `
# This is a comment
map-name system-username              database-username
foo      /^(.*)@cockroachlabs.com$    \1
bar      carl@cockroachlabs.com       also_carl   # Trailing comment
bar      carl@cockroachlabs.com       carl        # Duplicate behavior
`

	m, err := From(strings.NewReader(data))
	if !a.NoError(err) {
		return
	}
	t.Log(m.String())
	_, mapFound, _ := m.Map("asdf", "carl@cockroachlabs.com")
	a.False(mapFound)
	_, mapFound, _ = m.Map("fo", "carl@cockroachlabs.com")
	a.False(mapFound)
	_, mapFound, _ = m.Map("foo", "carl@cockroachlabs.com")
	a.True(mapFound)
	_, mapFound, _ = m.Map("bar", "something")
	a.True(mapFound)
}

func TestIdentityMapCaseInsensitive(t *testing.T) {
	a := assert.New(t)
	data := `
# Test case-insensitive matching for both literal and regex patterns.
# This is important because certificate CNs are normalized to lowercase
# before being passed to the user map.
certmap      AbC123DeF456                  cert_user
certmap      /^([A-Z0-9]+)@example.com$   user_\1
emailmap     /^(.*)@COMPANY.COM$           \1
`

	m, err := From(strings.NewReader(data))
	if !a.NoError(err) {
		return
	}

	// Test literal pattern matching is case-insensitive.
	elts, _, err := m.Map("certmap", "abc123def456")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("cert_user", elts[0].Normalized())
	}

	// Test uppercase version also matches.
	elts, _, err = m.Map("certmap", "ABC123DEF456")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("cert_user", elts[0].Normalized())
	}

	// Test mixed case also matches.
	elts, _, err = m.Map("certmap", "AbC123dEf456")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("cert_user", elts[0].Normalized())
	}

	// Test regex pattern matching is case-insensitive with substitution.
	elts, _, err = m.Map("certmap", "abc123@example.com")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("user_abc123", elts[0].Normalized())
	}

	// Test uppercase email also matches. Note that the captured group is lowercased
	// during SQL username normalization.
	elts, _, err = m.Map("certmap", "ABC123@EXAMPLE.COM")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("user_abc123", elts[0].Normalized())
	}

	// Test pattern with uppercase domain matches lowercase input.
	elts, _, err = m.Map("emailmap", "john.doe@company.com")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("john.doe", elts[0].Normalized())
	}

	// Test pattern with uppercase domain matches uppercase input.
	// The captured username is normalized to lowercase.
	elts, _, err = m.Map("emailmap", "JANE.DOE@COMPANY.COM")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("jane.doe", elts[0].Normalized())
	}
}

func TestIdentityMapCaseInsensitiveDeduplication(t *testing.T) {
	a := assert.New(t)
	data := `
# Test that deduplication works correctly with case-insensitive usernames.
# If multiple rules produce usernames that normalize to the same value,
# only the first one should be returned.
testmap      carl@cockroachlabs.com        carl
testmap      /^(.*)@cockroachlabs.com$     \1
`

	m, err := From(strings.NewReader(data))
	if !a.NoError(err) {
		return
	}

	// When we pass in Carl@CockroachLabs.com (note the different casing):
	// - First rule matches and produces: carl
	// - Second rule matches and produces: Carl (which normalizes to carl)
	// We should only get one result since they normalize to the same username.
	elts, _, err := m.Map("testmap", "Carl@CockroachLabs.com")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("carl", elts[0].Normalized())
	}

	// Also test with lowercase input to verify both rules match but deduplicate.
	elts, _, err = m.Map("testmap", "carl@cockroachlabs.com")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("carl", elts[0].Normalized())
	}

	// Test with a completely uppercase input.
	elts, _, err = m.Map("testmap", "CARL@COCKROACHLABS.COM")
	if a.NoError(err) && a.Len(elts, 1) {
		a.Equal("carl", elts[0].Normalized())
	}
}

func TestMapMultiple(t *testing.T) {
	t.Run("AllIdentitiesMatch", func(t *testing.T) {
		data := `
testmap      /^(.*)@example.com$           \1
testmap      /^(.*)@cockroachlabs.com$     crdb_\1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("testmap", []string{
			"user1@example.com",
			"user2@cockroachlabs.com",
		})
		assert.NoError(t, err)
		assert.True(t, mapFound)

		assert.Len(t, mappings, 2)
		assert.Equal(t, "user1@example.com", mappings[0].SystemIdentity)
		assert.Equal(t, "user1", mappings[0].MappedUser.Normalized())
		assert.Equal(t, "user2@cockroachlabs.com", mappings[1].SystemIdentity)
		assert.Equal(t, "crdb_user2", mappings[1].MappedUser.Normalized())
	})

	t.Run("SomeIdentitiesMatch", func(t *testing.T) {
		data := `
testmap      /^(.*)@example.com$           \1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("testmap", []string{
			"match@example.com",
			"nomatch@other.com",
			"another@example.com",
		})
		assert.NoError(t, err)
		assert.True(t, mapFound)

		assert.Len(t, mappings, 2)
		assert.Equal(t, "match@example.com", mappings[0].SystemIdentity)
		assert.Equal(t, "match", mappings[0].MappedUser.Normalized())
		assert.Equal(t, "another@example.com", mappings[1].SystemIdentity)
		assert.Equal(t, "another", mappings[1].MappedUser.Normalized())
	})

	t.Run("NoIdentitiesMatch", func(t *testing.T) {
		data := `
testmap      /^(.*)@example.com$           \1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("testmap", []string{
			"nomatch1",
			"nomatch2@other.com",
			"nomatch3",
		})

		assert.NoError(t, err)
		assert.True(t, mapFound)
		assert.Len(t, mappings, 0)
	})

	t.Run("DeduplicationSameUser", func(t *testing.T) {
		data := `
testmap      carl@example.com              carl
testmap      carl@cockroachlabs.com        carl
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("testmap", []string{
			"carl@cockroachlabs.com",
			"carl@example.com",
		})
		assert.NoError(t, err)
		assert.True(t, mapFound)

		// Only the first occurrence in map should be returned.
		// TODO: discuss this if map should be first or system identity order.
		assert.Len(t, mappings, 1)
		assert.Equal(t, "carl@cockroachlabs.com", mappings[0].SystemIdentity)
		assert.Equal(t, "carl", mappings[0].MappedUser.Normalized())
	})

	t.Run("DeduplicationCaseInsensitive", func(t *testing.T) {
		data := `
testmap      /^(.*)@example.com$           \1
testmap      /^(.*)@other.com$             \1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("testmap", []string{
			"Carl@example.com",
			"CARL@other.com",
		})
		assert.NoError(t, err)
		assert.True(t, mapFound)

		// Both produce "Carl" and "CARL" which normalize to "carl", so only first should be returned.
		assert.Len(t, mappings, 1)
		assert.Equal(t, "Carl@example.com", mappings[0].SystemIdentity)
		assert.Equal(t, "carl", mappings[0].MappedUser.Normalized())
	})

	t.Run("DeduplicationAcrossRules", func(t *testing.T) {
		data := `
testmap      carl@example.com              carl
testmap      /^(.*)@example.com$           \1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		// This identity matches both rules, both producing "carl".
		mappings, mapFound, err := m.MapMultiple("testmap", []string{"carl@example.com"})
		assert.NoError(t, err)
		assert.True(t, mapFound)

		// Only the first match should be returned.
		assert.Len(t, mappings, 1)
		assert.Equal(t, "carl@example.com", mappings[0].SystemIdentity)
		assert.Equal(t, "carl", mappings[0].MappedUser.Normalized())
	})

	t.Run("OrderFollowsElementPriority", func(t *testing.T) {
		data := `
testmap      user2@example.com             user2
testmap      user3@example.com             user3
testmap      user1@example.com             user1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		// Provide identities in a different order than rules are defined.
		mappings, mapFound, err := m.MapMultiple("testmap", []string{
			"user3@example.com",
			"user1@example.com",
			"user2@example.com",
		})
		assert.NoError(t, err)
		assert.True(t, mapFound)

		assert.Len(t, mappings, 3)
		// Results should follow system identity order (user3 is first).
		assert.Equal(t, "user3@example.com", mappings[0].SystemIdentity)
		assert.Equal(t, "user3", mappings[0].MappedUser.Normalized())
		assert.Equal(t, "user1@example.com", mappings[1].SystemIdentity)
		assert.Equal(t, "user1", mappings[1].MappedUser.Normalized())
		assert.Equal(t, "user2@example.com", mappings[2].SystemIdentity)
		assert.Equal(t, "user2", mappings[2].MappedUser.Normalized())
	})

	t.Run("FirstMatchWins", func(t *testing.T) {
		data := `
testmap      identity1                     target_user
testmap      identity2                     target_user
testmap      identity3                     target_user
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("testmap", []string{
			"identity2",
			"identity1",
			"identity3",
		})
		assert.NoError(t, err)
		assert.True(t, mapFound)
		// All map to same user, but first match (based on system identity order) wins.
		// System identities are ordered: identity2, identity1, identity3
		// For each identity, we check all mapped rules.
		// So rule1 (identity2) checks all identities -> matches "identity2"
		assert.Len(t, mappings, 1)
		assert.Equal(t, "identity2", mappings[0].SystemIdentity)
		assert.Equal(t, "target_user", mappings[0].MappedUser.Normalized())
	})

	t.Run("NonExistentMap", func(t *testing.T) {
		data := `
testmap      /^(.*)@example.com$           \1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("nonexistent", []string{
			"user@example.com",
		})
		assert.NoError(t, err)
		assert.False(t, mapFound)
		assert.Len(t, mappings, 0)
	})

	t.Run("EmptyConfiguration", func(t *testing.T) {
		m := Empty()

		mappings, mapFound, err := m.MapMultiple("testmap", []string{
			"user@example.com",
		})
		assert.NoError(t, err)
		assert.False(t, mapFound)
		assert.Len(t, mappings, 0)
	})

	t.Run("CertificateSANs", func(t *testing.T) {
		data := `
sanmap       /^SAN:DNS:(.*)\.example\.com$     domain_\1
sanmap       /^SAN:URI:https://(.*)$            uri_\1
sanmap       /^SAN:IP:(.*)$                     ip_\1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("sanmap", []string{
			"SAN:DNS:www.example.com",
			"SAN:DNS:api.example.com",
			"SAN:URI:https://service.internal",
			"SAN:IP:192.168.1.1",
			"SAN:EMAIL:user@example.com", // This won't match any rule.
		})
		assert.NoError(t, err)
		assert.True(t, mapFound)

		assert.Len(t, mappings, 4)
		assert.Equal(t, "SAN:DNS:www.example.com", mappings[0].SystemIdentity)
		assert.Equal(t, "domain_www", mappings[0].MappedUser.Normalized())
		assert.Equal(t, "SAN:DNS:api.example.com", mappings[1].SystemIdentity)
		assert.Equal(t, "domain_api", mappings[1].MappedUser.Normalized())
		assert.Equal(t, "SAN:URI:https://service.internal", mappings[2].SystemIdentity)
		assert.Equal(t, "uri_service.internal", mappings[2].MappedUser.Normalized())
		assert.Equal(t, "SAN:IP:192.168.1.1", mappings[3].SystemIdentity)
		assert.Equal(t, "ip_192.168.1.1", mappings[3].MappedUser.Normalized())
	})

	t.Run("ComplexRuleSet", func(t *testing.T) {
		data := `
complexmap   admin@cockroachlabs.com       admin
complexmap   /^(.*)@cockroachlabs.com$     crdb_\1
complexmap   /^(.*)@example.com$           ext_\1
complexmap   /^service-account-.*$         service
complexmap   /^test-(.*)$                  test_\1
`
		m, err := From(strings.NewReader(data))
		assert.NoError(t, err)

		mappings, mapFound, err := m.MapMultiple("complexmap", []string{
			"admin@cockroachlabs.com",
			"alice@cockroachlabs.com",
			"bob@example.com",
			"service-account-123",
			"test-user",
			"unknown",
		})
		assert.NoError(t, err)
		assert.True(t, mapFound)

		// admin@cockroachlabs.com matches both rule 1 (→admin) and rule 2 (→crdb_admin),
		// producing two different users, so we get 6 mappings total.
		assert.Len(t, mappings, 6)
		// Results follow rule order.
		// Rule 1: admin@cockroachlabs.com → admin
		assert.Equal(t, "admin@cockroachlabs.com", mappings[0].SystemIdentity)
		assert.Equal(t, "admin", mappings[0].MappedUser.Normalized())
		// Rule 2: /^(.*)@cockroachlabs.com$/ → crdb_\1 (matches admin→crdb_admin and alice→crdb_alice)
		assert.Equal(t, "admin@cockroachlabs.com", mappings[1].SystemIdentity)
		assert.Equal(t, "crdb_admin", mappings[1].MappedUser.Normalized())
		assert.Equal(t, "alice@cockroachlabs.com", mappings[2].SystemIdentity)
		assert.Equal(t, "crdb_alice", mappings[2].MappedUser.Normalized())
		// Rule 3: /^(.*)@example.com$/ → ext_\1 (matches bob@example.com)
		assert.Equal(t, "bob@example.com", mappings[3].SystemIdentity)
		assert.Equal(t, "ext_bob", mappings[3].MappedUser.Normalized())
		// Rule 4: /^service-account-.*$/ → service (matches service-account-123)
		assert.Equal(t, "service-account-123", mappings[4].SystemIdentity)
		assert.Equal(t, "service", mappings[4].MappedUser.Normalized())
		// Rule 5: /^test-(.*)$/ → test_\1 (matches test-user)
		assert.Equal(t, "test-user", mappings[5].SystemIdentity)
		assert.Equal(t, "test_user", mappings[5].MappedUser.Normalized())
	})
}
