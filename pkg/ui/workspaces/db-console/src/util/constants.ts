// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { util } from "@cockroachlabs/cluster-ui";

export const indexNameAttr = "index_name";

export const {
  aggregatedTsAttr,
  appAttr,
  appNamesAttr,
  dashQueryString,
  dashboardNameAttr,
  databaseAttr,
  databaseNameAttr,
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
  txnFingerprintIdAttr,
  unset,
  viewAttr,
  REMOTE_DEBUGGING_ERROR_TEXT,
  idAttr,
} = util;
