// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import sample from "lodash/sample";

import { randomName } from "./randomName";

export function randomRole(): string {
  return sample(["root", "admin", "public", randomName()]);
}
