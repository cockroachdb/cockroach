// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	_ "github.com/lib/pq"
)

func updateRegserver(dbURL string, version *semver.Version) error {
	// The format of the entry looks like this:
	// select * from registration.versions;
	//     version     |                             details
	//-----------------+-------------------------------------------------------------------
	//  21.2.14        | https://www.cockroachlabs.com/docs/releases/v21.2#v21-2-14
	db, err := sql.Open("postgres", dbURL)
	url := fmt.Sprintf("https://www.cockroachlabs.com/docs/releases/v%d.%d#%s", version.Major(), version.Minor(), strings.ReplaceAll(version.Original(), ".", "-"))
	if err != nil {
		return fmt.Errorf("sql driver: %w", err)
	}
	if _, err := db.Exec("INSERT INTO registration.versions (version, details) VALUES ($1, $2)", version.String(), url); err != nil {
		return fmt.Errorf("sql exec: %w", err)
	}

	return nil
}
