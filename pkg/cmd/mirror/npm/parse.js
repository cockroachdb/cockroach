// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const fs = require("fs");
const lockfile = require("@yarnpkg/lockfile");

// Remove "node" and this filename.
const argv = process.argv.slice(2);

// Vailate the one and only positional argument.
if (argv.length !== 1) {
  const path = require("path");
  const invoked = path.basename(process.argv[1]);
  console.error(
    `
${invoked}: Prints a yarn.lock file in a JSON representation for use in other programs.

Usage: ${invoked} YARN_LOCK

Examples:
    ${invoked} ./yarn.lock
    ${invoked} pkg/ui/workspaces/cluster-ui/yarn.lock
    `.trim());
  process.exit(1);
}

const filename = argv[0];

// Read and parse the file, letting errors throw.
const contents = fs.readFileSync(filename, { encoding: "utf-8" });
const parsed = lockfile.parse(contents, filename);

// Handle the result.
switch (parsed.type) {
  case "merge":
    console.error("Merge marker detected, but file still parsed successfully.");
    console.error("To avoid false-positive results, this is considered an error.");
    process.exitCode = 2;
    break;
  case "conflict":
    console.error("Merge marker detected, and file couldn't be parsed.");
    process.exitCode = 3;
    break;
  case "success":
    console.log(JSON.stringify(parsed.object));
}
