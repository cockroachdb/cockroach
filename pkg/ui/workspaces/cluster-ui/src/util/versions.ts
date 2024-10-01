// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { getLogger } from "./logger";

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
  if (inputString.startsWith(versionPrefix)) {
    inputString = inputString.split(versionPrefix)[1];
    if (!inputString) {
      getLogger().warn(
        "Unable to split version string while parsing: " + inputString,
      );
      return [0, 0, 0];
    }
  }

  const regex = /^(\d{2})(?:\.(\d{1}))?(?:\.(\d{1}))?(-\d+|[A-Za-z]+)?(.\d+)?/;
  const matches = inputString.match(regex);

  if (matches) {
    const [, majorVersion, minorVersion, patchVersion] = matches;
    const parsedMajorVersion = parseInt(majorVersion);
    const parsedMinorVersion = parseInt(minorVersion) || 0;
    const parsedPatchVersion = parseInt(patchVersion) || 0;

    return [parsedMajorVersion, parsedMinorVersion, parsedPatchVersion];
  } else {
    getLogger().warn("Version string did not match: " + inputString);
    return [0, 0, 0];
  }
}
