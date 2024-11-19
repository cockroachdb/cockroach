// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Identity } from "../network";

export interface NoConnection {
  from: Identity;
  to: Identity;
}
