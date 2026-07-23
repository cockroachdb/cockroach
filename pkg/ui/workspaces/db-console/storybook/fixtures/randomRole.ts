// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import _ from "lodash";

import { randomName } from "./randomName";

export function randomRole(): string {
  return _.sample(["root", "admin", "public", randomName()]);
}
