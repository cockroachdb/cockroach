// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

export interface Identity {
  nodeID: number;
  address: string;
  locality?: string;
  updatedAt: moment.Moment;
}
