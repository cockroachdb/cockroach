// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

let path = "";

export const setBasePath = (basePath: string): string => (path = basePath);

// Do not call this directly, use `withBasePath` instead because it
// ensures that if the basePath is blank the paths remain relative.
const getBasePath = (): string => path;

export const withBasePath = (p: string): string =>
  joinPathsEnsureRelative(getBasePath(), p);

// joinPathsEnsureRelative is a utility to ensure that paths relative
// to base don't have a prefix `/` to remain compatible with an empty
// relative basePath. It ensures that we append it to a base correctly,
// regardless of trailing slash on the base path.
//
// Examples:
//  joinPathsEnsureRelative("",  "/_status") -> Error! Not a relative path.
//  joinPathsEnsureRelative("/", "/_status") -> Error! Not a relative path.
//  joinPathsEnsureRelative("",  "_status") -> "_status"
//  joinPathsEnsureRelative("/", "_status") -> "/_status"
//  joinPathsEnsureRelative("dbconsole", "_status") -> "dbconsole/_status"
//
export const joinPathsEnsureRelative = (
  left: string,
  right: string,
): string => {
  if (right.startsWith("/")) {
    throw new Error(
      "Application paths must remain compatible with relative base. Remove prefix `/` character.",
    );
  }
  if (left === "") {
    return right;
  }
  if (left.endsWith("/")) {
    return left.concat(right);
  } else {
    return left.concat("/", right);
  }
};
