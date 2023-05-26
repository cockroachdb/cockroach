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

import "github.com/Masterminds/semver/v3"

func updateBrew(workDir string, version *semver.Version, latestMajor bool) error {
	// cockroach@major.minor is supported for all releases
	//commands := []*exec.Cmd{
	//	exec.Command("make", fmt.Sprintf("VERSION=%s", version.String()), fmt.Sprintf("PRODUCT=cockroach@%d.%d", version.Major(), version.Minor())),
	//}
	//// limited to the latest release only
	//if latestMajor {
	//	commands = append(commands, exec.Command("make", fmt.Sprintf("VERSION=%s", version.String()), "PRODUCT=cockroach"))
	//	commands = append(commands, exec.Command("make", fmt.Sprintf("VERSION=%s", version.String()), "PRODUCT=cockroach-sql"))
	//}
	return nil
}
