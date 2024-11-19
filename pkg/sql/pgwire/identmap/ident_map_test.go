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
			pattern:      regexp.MustCompile("^" + regexp.QuoteMeta(sysName) + "$"),
			substituteAt: -1,
		}
	}
	regexMatch := func(sysName, dbName string) element {
		return element{
			dbUser:       dbName,
			pattern:      regexp.MustCompile(sysName),
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
