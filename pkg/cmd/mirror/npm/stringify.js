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
${invoked}: Prints the provided JSON representation of a yarn.lock file back in its standard yarn.lock format.

Usage: ${invoked} YARN_LOCK_JSON

Examples:
    ${invoked} ./yarn.lock.json
    ${invoked} pkg/ui/workspaces/cluster-ui/yarn.lock.json
    `.trim());
  process.exit(1);
}

const filename = argv[0];

// Read and parse the file, letting errors throw.
const contents = fs.readFileSync(filename, { encoding: "utf-8" });
const parsed = JSON.parse(contents);

// Then convert to yarn.lock and print.
console.log(lockfile.stringify(parsed));

