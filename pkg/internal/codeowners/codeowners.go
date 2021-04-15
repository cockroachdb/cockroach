// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package codeowners

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/gopath"
	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/errors"
	"github.com/zabawaba99/go-gitignore"
)

// DefaultCodeOwnersLocation is the default location for the CODEOWNERS file.
var DefaultCodeOwnersLocation = filepath.Join(
	gopath.Get(),
	"src",
	"github.com",
	"cockroachdb",
	"cockroach",
	".github",
	"CODEOWNERS",
)

// Rule is a single rule within a CODEOWNERS file.
type Rule struct {
	Pattern string
	Owners  []team.Alias
}

// CodeOwners is a struct encapsulating a CODEOWNERS file.
type CodeOwners struct {
	rules []Rule
	teams map[team.Alias]team.Team
}

// LoadCodeOwners parses a CODEOWNERS file and returns the CodeOwners struct.
func LoadCodeOwners(r io.Reader, teams map[team.Alias]team.Team) (*CodeOwners, error) {
	s := bufio.NewScanner(r)
	ret := &CodeOwners{
		teams: teams,
	}
	lineNum := 1
	for s.Scan() {
		lineNum++
		if s.Err() != nil {
			return nil, s.Err()
		}
		t := s.Text()
		if strings.HasPrefix(t, "#") {
			continue
		}
		t = strings.TrimSpace(t)
		if len(t) == 0 {
			continue
		}

		fields := strings.Fields(t)
		rule := Rule{Pattern: fields[0]}
		for _, field := range fields[1:] {
			// @cockroachdb/kv[-noreview] --> cockroachdb/kv.
			owner := team.Alias(strings.TrimSuffix(strings.TrimPrefix(field, "@"), "-noreview"))

			if _, ok := teams[owner]; !ok {
				return nil, errors.Newf("owner %s does not exist", owner)
			}
			rule.Owners = append(rule.Owners, owner)
		}
		ret.rules = append(ret.rules, rule)
	}
	return ret, nil
}

// DefaultLoadCodeOwners loads teams from the DefaultCodeOwnersLocation.
func DefaultLoadCodeOwners() (*CodeOwners, error) {
	teams, err := team.DefaultLoadTeams()
	if err != nil {
		return nil, err
	}
	f, err := os.Open(DefaultCodeOwnersLocation)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return LoadCodeOwners(f, teams)
}

var repoRoot = filepath.Join(gopath.Get(), "src", "github.com", "cockroachdb", "cockroach")

// Match matches the given file to the rules and returns the owning team(s).
// If the path is relative, it is interpreted as relative to the repository
// root (i.e. like in the CODEOWNERS file).
//
// Returns empty if there are no owning teams.
func (co *CodeOwners) Match(filePath string) []team.Team {
	if filepath.IsAbs(filePath) {
		var err error
		// Strip repoRoot prefix.
		filePath, err = filepath.Rel(repoRoot, filePath)
		if err != nil {
			_ = err
			return nil
		}
	}
	filePath = string(filepath.Separator) + filepath.Clean(filePath)
	// filePath is now "absolute relative to the repo root", i.e.
	// `/pkg/acceptance`, and the cleaning helps avoid inputs like
	// `./pkg/acceptance` not working (when `pkg/acceptance` would).
	//
	// Keep matching until we hit the root directory.
	lastFilePath := ""
	for filePath != lastFilePath {
		// Rules are matched backwards.
		for i := len(co.rules) - 1; i >= 0; i-- {
			rule := co.rules[i]
			// For subdirectories, CODEOWNERS will add ** automatically for matches.
			// As such, if the pattern ends with a directory (i.e. '/'), add the ** operator implicitly.
			if gitignore.Match(rule.Pattern, filePath) {
				teams := make([]team.Team, len(rule.Owners))
				for i, owner := range rule.Owners {
					teams[i] = co.teams[owner]
				}
				return teams
			}
		}
		lastFilePath = filePath
		filePath = filepath.Dir(filePath)
	}
	return nil
}
