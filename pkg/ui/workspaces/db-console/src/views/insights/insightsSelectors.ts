// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  selectTransactionFingerprintID,
  util,
} from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";

import { AdminUIState } from "src/redux/state";

export const selectTransactionInsights = (state: AdminUIState) =>
  state.cachedData.txnInsights?.valid
    ? state.cachedData.txnInsights?.data?.results
    : null;

export const selectTxnInsightsByFingerprint = createSelector(
  selectTransactionInsights,
  selectTransactionFingerprintID,
  (execInsights, fingerprintID) => {
    if (fingerprintID == null) {
      return null;
    }
    const id = util.FixFingerprintHexValue(BigInt(fingerprintID).toString(16));
    return execInsights?.filter(txn => txn.transactionFingerprintID === id);
  },
);
