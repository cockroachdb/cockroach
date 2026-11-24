// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { StatementDetailsResponse } from "../api";

// resolveQuery returns the statement fingerprint text, preferring the
// top-level Query field on the response and falling back to the deprecated
// metadata.formatted_query / metadata.query fields for older servers.
export const resolveQuery = (
  details: StatementDetailsResponse | undefined,
): string =>
  details?.query ||
  details?.statement?.metadata?.formatted_query ||
  details?.statement?.metadata?.query ||
  "";

// resolveQuerySummary returns the abbreviated statement, preferring the
// top-level QuerySummary field with a fallback to metadata.query_summary.
export const resolveQuerySummary = (
  details: StatementDetailsResponse | undefined,
): string =>
  details?.query_summary || details?.statement?.metadata?.query_summary || "";

// resolveDatabase returns the database name, preferring the top-level
// Database field with a fallback to metadata.databases[0].
export const resolveDatabase = (
  details: StatementDetailsResponse | undefined,
): string =>
  details?.database || details?.statement?.metadata?.databases?.[0] || "";
