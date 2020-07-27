// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package owner deciphers OWNERS files in the file tree to determine
// which team owns a given file. The OWNERS file is in YAML format.
//
// An OWNERS file is structured as the following:
//  owners:
//    [sequence of teams found in TEAMS]
//  files:
//    file_name_or_unix_wildcard:
//       [sequence of teams found in TEAMS]
//
// As an example, say in /dir/OWNERS there is a structure yaml file /dir/OWNERS:
//  owners:
//   - team_a
//   - team_b
//  files:
//   afile*:
//    owners:
//      - team_c
//   file_b:
//    owners:
//      - team_b
//
// And in a subdirectory, /dir/subdir/OWNERS contains:
//  owners:
//    -team_d
//
// Then the ownership for the given directory is as such:
//   /dir (team_a, team_b)
//     /subdir (team_d)
//        file_a (team_d)
//        file_b (team_d)
//        OWNERS
//        /subsubdir (team_d)
//           file_a (team_d)
//     /subdir_2 (team_a, team_b)
//        file_a (team_a, team_b)
//     file_a (team_a, team_b)
//     file_b (team_b)
//     file_c (team_a, team_b)
//     afile_a (team_c)
//     afile_b (team_c)
//     OWNERS
package owner

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/internal/team"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

// FileExpression is either a filename, or a wildcard for a set of files.
// Wildcards follows UNIX semantics, e.g. *file* matches afile, afileb, fileb and file.
type FileExpression string

// OwnersYAMLBase contains the base for all OWNERS fields.
type OwnersYAMLBase struct {
	Owners []team.Name `yaml:"owners"`
}

// OwnersYAML is the struct of the OWNERS file
type OwnersYAML struct {
	Owners []team.Name                       `yaml:"owners"`
	Files  map[FileExpression]OwnersYAMLBase `yaml:"files,omitempty"`
	OwnersYAMLBase
}

// Owners contains metadata about ownership for a given directory.
type Owners struct {
	// Owners is a list of owners for the team directory.
	// This can be superceded by "FileOwners".
	Owners []team.Team
	// FileOwners is a map from a file name wildcard expression
	// to a list of owners.
	FileOwners map[FileExpression][]team.Team
}

// LoadOwners loads the contents of an io.Reader and returns ownership information.
func LoadOwners(r io.Reader, teams map[team.Name]team.Team) (*Owners, error) {
	contents, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var ownersIn OwnersYAML
	if err := yaml.UnmarshalStrict(contents, &ownersIn); err != nil {
		return nil, err
	}
	ret := &Owners{
		Owners:     make([]team.Team, len(ownersIn.Owners)),
		FileOwners: make(map[FileExpression][]team.Team, len(ownersIn.Files)),
	}
	for i, owner := range ownersIn.Owners {
		t, ok := teams[owner]
		if !ok {
			return nil, errors.Newf("failed to find owner team: %s", owner)
		}
		ret.Owners[i] = t
	}
	for fileExpr, ownerInfo := range ownersIn.Files {
		fileExprTeams := make([]team.Team, len(ownerInfo.Owners))
		for i, owner := range ownerInfo.Owners {
			t, ok := teams[owner]
			if !ok {
				return nil, errors.Newf("failed to find owner team: %s", owner)
			}
			fileExprTeams[i] = t
		}
		ret.FileOwners[fileExpr] = fileExprTeams
	}
	return ret, nil
}

// FindOwners finds the owner(s) of a given file or directory.
func FindOwners(fileOrDir string, teams map[team.Name]team.Team) ([]team.Team, error) {
	// Start at the file.
	prevDirOrFile := ""
	currentDir := fileOrDir
	stats, err := os.Stat(fileOrDir)
	if err != nil {
		return nil, err
	}
	// If it's a file, we already found the dir.
	if !stats.IsDir() {
		prevDirOrFile = fileOrDir
		currentDir = filepath.Dir(fileOrDir)
	}
	// Loop through each directory and find if they have OWNERS.
	for currentDir != prevDirOrFile {
		f, err := os.Open(filepath.Join(currentDir, "OWNERS"))
		if err != nil {
			if os.IsNotExist(err) {
				prevDirOrFile = currentDir
				currentDir = filepath.Dir(currentDir)
				continue
			}
			return nil, err
		}
		defer func() { _ = f.Close() }()

		loadedOwners, err := LoadOwners(f, teams)
		if err != nil {
			return nil, err
		}

		// First, check if the file is individually ownered.
		basename := filepath.Base(prevDirOrFile)
		for expr, owners := range loadedOwners.FileOwners {
			matched, err := filepath.Match(string(expr), basename)
			if err != nil {
				return nil, err
			}
			if matched {
				return owners, nil
			}
		}
		// If nothing matches, return the owners at the root.
		return loadedOwners.Owners, nil
	}
	return nil, nil
}
