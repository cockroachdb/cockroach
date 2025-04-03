// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package identmap contains the code for parsing a pg_ident.conf file,
// which allows a database operator to create some number of mappings
// between system identities (e.g.: GSSAPI or X.509 principals) and
// database usernames.
package identmap

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
	"github.com/olekukonko/tablewriter"
)

// Conf provides a multi-level, user-configurable mapping between an
// external system identity (e.g.: GSSAPI or X.509 principals) and zero
// or more database usernames which the external principal may act as.
//
// The Conf supports being initialized from a file format that
// is compatible with Postgres's pg_ident.conf file:
//
//	# Comments
//	map-name system-identity    database-username
//	# Convert "carl@example.com" ==> "example-carl"
//	map-name /^(.*)@example.com$  example-\1
//
// If the system-identity field starts with a slash, it will be
// interpreted as a regular expression. The system-identity expression
// may include a single capturing group, which may be substituted into
// database-username with the character sequence \1 (backslash one). The
// regular expression will be un-anchored for compatibility; users are
// therefore encouraged to always specify anchors to eliminate ambiguity.
//
// See also: https://www.postgresql.org/docs/13/auth-username-maps.html
type Conf struct {
	// data is keyed by the map-map of a pg_ident entry.
	data map[string][]element
	// originalLines is for debugging use.
	originalLines []string
	// sortedKeys is for debugging use, to make String() deterministic.
	sortedKeys []string
}

// Empty returns an empty configuration.
func Empty() *Conf {
	return &Conf{}
}

// linePattern is just three columns of data, so let's be
// lazy and use a regexp instead of a full-blown parser.
var linePattern = regexp.MustCompile(`^(\S+)\s+(\S+)\s+(\S+)$`)

// From parses a reader containing a pg_ident.conf file.
func From(r io.Reader) (*Conf, error) {
	ret := &Conf{data: make(map[string][]element)}
	scanner := bufio.NewScanner(r)
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		ret.originalLines = append(ret.originalLines, line)

		line = tidyIdentMapLine(line)
		if line == "" {
			continue
		}

		parts := linePattern.FindStringSubmatch(line)
		if len(parts) != 4 {
			return nil, errors.Errorf("unable to parse line %d: %q", lineNo, line)
		}
		mapName := parts[1]

		var sysPattern *regexp.Regexp
		var err error
		if sysName := parts[2]; sysName[0] == '/' {
			sysPattern, err = regexp.Compile(sysName[1:])
		} else {
			sysPattern, err = regexp.Compile("^" + regexp.QuoteMeta(sysName) + "$")
		}
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse line %d", lineNo)
		}

		dbUser := parts[3]
		subIdx := strings.Index(dbUser, `\1`)
		if subIdx >= 0 {
			if sysPattern.NumSubexp() == 0 {
				return nil, errors.Errorf(
					`saw \1 substitution on line %d, but pattern contains no subexpressions`, lineNo)
			}
		}

		elt := element{
			dbUser:       dbUser,
			pattern:      sysPattern,
			substituteAt: subIdx,
		}
		if existing, ok := ret.data[mapName]; ok {
			ret.data[mapName] = append(existing, elt)
		} else {
			ret.sortedKeys = append(ret.sortedKeys, mapName)
			ret.data[mapName] = []element{elt}
		}
	}
	sort.Strings(ret.sortedKeys)
	return ret, nil
}

// Empty returns true if no mappings have been defined.
func (c *Conf) Empty() bool {
	return len(c.data) == 0
}

// Map returns the database usernames that a system identity maps to
// within the named mapping. If there are no matching usernames, or if
// mapName is unknown, nil will be returned. The returned list will be
// ordered based on the order in which the rules were defined.  If there
// are rules which generate identical mappings, only the first one will
// be returned. That is, the returned list will be deduplicated,
// preferring the first instance of any given username.
// A boolean will be returned which indicates if there are any rows that
// correspond to the given mapName.
func (c *Conf) Map(mapName, systemIdentity string) ([]username.SQLUsername, bool, error) {
	if c.data == nil {
		return nil, false, nil
	}
	elts := c.data[mapName]
	if elts == nil {
		return nil, false, nil
	}
	var names []username.SQLUsername
	seen := make(map[string]bool)
	for _, elt := range elts {
		if n := elt.substitute(systemIdentity); n != "" && !seen[n] {
			// We're returning this as a for-validation username since a
			// pattern-based mapping could still result in invalid characters
			// being incorporated into the input.
			u, err := username.MakeSQLUsernameFromUserInput(n, username.PurposeValidation)
			if err != nil {
				return nil, true, err
			}
			names = append(names, u)
			seen[n] = true
		}
	}
	return names, true, nil
}

func (c *Conf) String() string {
	if len(c.data) == 0 {
		return "# (empty configuration)"
	}
	var sb strings.Builder
	sb.WriteString("# Original configuration:\n")
	for _, l := range c.originalLines {
		fmt.Fprintf(&sb, "# %s\n", l)
	}
	sb.WriteString("# Active configuration:\n")
	table := tablewriter.NewWriter(&sb)
	table.SetAutoWrapText(false)
	table.SetReflowDuringAutoWrap(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(false)
	table.SetNoWhiteSpace(true)
	table.SetTrimWhiteSpaceAtEOL(true)
	table.SetTablePadding(" ")

	row := []string{"# map-name", "system-username", "database-username", ""}
	table.Append(row)
	for _, k := range c.sortedKeys {
		row[0] = k
		for _, elt := range c.data[k] {
			row[1] = elt.pattern.String()
			row[2] = elt.dbUser
			if elt.substituteAt == -1 {
				row[3] = ""
			} else {
				row[3] = fmt.Sprintf("# substituteAt=%d", elt.substituteAt)
			}
			table.Append(row)
		}
	}
	table.Render()
	return sb.String()
}

type element struct {
	dbUser string
	// pattern may just be a literal match.
	pattern *regexp.Regexp
	// If substituteAt is non-negative, it indicates the index at which
	// the \1 substitution token occurs. This also implies that pattern
	// has at least one submatch in it.
	substituteAt int
}

// substitute returns a non-empty string if the map element matches the
// external system username.
func (e element) substitute(systemUsername string) string {
	m := e.pattern.FindStringSubmatch(systemUsername)
	if m == nil {
		return ""
	}
	if e.substituteAt == -1 {
		return e.dbUser
	}
	return e.dbUser[0:e.substituteAt] + m[1] + e.dbUser[e.substituteAt+2:]
}

// tidyIdentMapLine removes # comments and trims whitespace.
func tidyIdentMapLine(line string) string {
	if commentIdx := strings.IndexByte(line, '#'); commentIdx != -1 {
		line = line[0:commentIdx]
	}

	return strings.TrimSpace(line)
}
