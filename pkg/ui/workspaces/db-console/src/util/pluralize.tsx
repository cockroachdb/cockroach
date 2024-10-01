// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

export function pluralize(value: number, singular: string, plural: string) {
  if (value === 1) {
    return singular;
  }
  return plural;
}
