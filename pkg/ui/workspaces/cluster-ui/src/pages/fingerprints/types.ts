// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

export type FingerprintRow = {
  key: string;
  fingerprint: string;
  query: string;
  summary: string;
  implicitTxn: boolean;
  database: string;
  createdAt: moment.Moment;
};
