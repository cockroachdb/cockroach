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
