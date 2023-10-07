// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { HexStringToByteArray, makeTimestamp } from "../util";
import {
  TransactionExecutionInsightsRequest,
  TxnInsightsRequest,
} from "./txnInsightsApi";
import { fromNumber, fromString } from "long";

export function createTxnInsightsReq(
  req?: TxnInsightsRequest,
): TransactionExecutionInsightsRequest {
  const fingerprintID = req.txnFingerprintID
    ? fromString(req.txnFingerprintID)
    : fromNumber(0);
  const execID = req.txnExecutionID
    ? HexStringToByteArray(req.txnExecutionID)
    : null;
  const start = req.start ? makeTimestamp(req.start.unix()) : null;
  const end = req.end ? makeTimestamp(req.end.unix()) : null;

  return {
    txn_fingerprint_id: fingerprintID,
    transaction_id: execID,
    start_time: start,
    end_time: end,
    with_contention_events: false,
    // Set to true so we can retrieve the transaction query on the overview page.
    with_statement_insights: true,
  };
}
