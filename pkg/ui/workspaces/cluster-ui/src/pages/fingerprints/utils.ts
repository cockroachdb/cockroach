// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { StatementFingerprint } from "src/api/statementFingerprintsApi";

import { FingerprintRow } from "./types";

export const statementFingerprintsToRows = (
  fingerprints: StatementFingerprint[],
): FingerprintRow[] => {
  return fingerprints.map(fp => ({
    key: fp.fingerprint,
    fingerprint: fp.fingerprint,
    query: fp.query,
    summary: fp.summary,
    implicitTxn: fp.implicitTxn,
    database: fp.database,
    createdAt: fp.createdAt,
  }));
};
