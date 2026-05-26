// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";

export const indexNameAttr = "index_name";
export const tenantNameAttr = "tenant";

export const {
  aggregatedTsAttr,
  appAttr,
  appNamesAttr,
  dashQueryString,
  dashboardNameAttr,
  databaseAttr,
  databaseNameAttr,
  databaseIDAttr,
  fingerprintIDAttr,
  executionIdAttr,
  implicitTxnAttr,
  nodeIDAttr,
  nodeQueryString,
  rangeIDAttr,
  statementAttr,
  sessionAttr,
  tabAttr,
  tableNameAttr,
  tableIdAttr,
  txnFingerprintIdAttr,
  unset,
  viewAttr,
  REMOTE_DEBUGGING_ERROR_TEXT,
  idAttr,
  indexUnusedDuration,
} = util;
