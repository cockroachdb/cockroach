// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const versionPrefix = "10000";
export type SemVersion = [number, number, number];

// greaterOrEqualThanVersion cribbed from managed-service.
export const greaterOrEqualThanVersion = (
  version: string,
  [otherMajor, otherMinor, otherPatch]: SemVersion,
) => {
  const [major, minor, patch] = parseStringToVersion(version);
  if (
    major === undefined ||
    minor === undefined ||
    isNaN(major) ||
    isNaN(minor)
  ) {
    return false;
  }
  if (major > otherMajor) {
    return true;
  }
  if (major < otherMajor) {
    return false;
  }
  if (minor > otherMinor) {
    return true;
  }
  if (minor < otherMinor) {
    return false;
  }
  // if only patch version isn't specified, consider it as a latest version.
  // patch part might be omitted for "latest" versions (ie latest-21.1-build)
  if (patch === undefined || isNaN(patch) || patch >= otherPatch) {
    return true;
  }
  return false;
};

export function parseStringToVersion(
  inputString: string,
): [number, number, number] {
  const strippedInputString = inputString.split(versionPrefix)[1];
  if (!strippedInputString) {
    console.log("Unable to split version string while parsing", inputString);
    return [0, 0, 0];
  }
  const regex = /^(\d{2})(?:\.(\d{1}))?(?:\.(\d{1}))?(-\d+|[A-Za-z]+)?(.\d+)?/;
  const matches = strippedInputString.match(regex);

  if (matches) {
    const [, majorVersion, minorVersion, patchVersion, buildNumber] = matches;
    const parsedMajorVersion = parseInt(majorVersion);
    const parsedMinorVersion = parseInt(minorVersion) || 0;
    const parsedPatchVersion = parseInt(patchVersion) || 0;
    const parsedBuildNumber = parseInt(buildNumber) || 0;

    return [parsedMajorVersion, parsedMinorVersion, parsedPatchVersion];
  } else {
    console.log("Version string did not match", inputString);
    return [0, 0, 0];
  }
}
