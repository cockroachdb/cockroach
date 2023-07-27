// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "@reduxjs/toolkit";
import {
  appNamesAttr,
  getMatchParamByName,
  queryByName,
  txnFingerprintIdAttr,
  unset,
} from "../util";
import { SqlStatsResponse } from "../api";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { match } from "react-router";
import { Location } from "history";
import { statementFingerprintIdsToText } from "../transactionsPage/utils";

type Transaction =
  cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

type Statement =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

/**
 * getTxnFromSqlStatsTxns returns the txn from the txns list with the
 * specified txn fingerprint ID and app anme.
 *
 * @param txns List of txns to search through.
 * @param txnFingerprintID
 * @param apps
 */
export const getTxnFromSqlStatsTxns = (
  txns: Transaction[] | null,
  txnFingerprintID: string | null,
  apps: string[] | null,
): Transaction | null => {
  if (!txns?.length || !txnFingerprintID) {
    return null;
  }

  return txns.find(
    txn =>
      txn.stats_data.transaction_fingerprint_id.toString() ===
        txnFingerprintID &&
      (apps?.length ? apps.includes(txn.stats_data.app ?? unset) : true),
  );
};

export const getTxnFromSqlStatsMemoized = createSelector(
  (resp: SqlStatsResponse, _match: match, _location: Location): Transaction[] =>
    resp?.transactions ?? [],
  (_resp, _match, location): string[] =>
    queryByName(location, appNamesAttr)
      ?.split(",")
      .map(s => s.trim()) ?? [],
  (_resp, routeMatch, _location: Location): string =>
    getMatchParamByName(routeMatch, txnFingerprintIdAttr),
  (txns, apps, fingerprintID): Transaction => {
    return getTxnFromSqlStatsTxns(txns, fingerprintID, apps);
  },
);

/**
 * getTxnQueryString returns the transaction query built from the provided stmt list
 * @param txn Transaction to build query for.
 * @param stmts List of stmts from which to get query strings.
 */
export const getTxnQueryString = (
  txn: Transaction | null,
  stmts: Statement[],
): string => {
  if (!txn || !stmts?.length) return "";

  const statementFingerprintIds = txn?.stats_data?.statement_fingerprint_ids;

  return (
    (statementFingerprintIds &&
      statementFingerprintIdsToText(statementFingerprintIds, stmts)) ??
    ""
  );
};

/**
 * getStatementsForTransaction returns the list of stmts with transaction ids
 * matching the txn id of the provided txn.
 * @param txn Txn for which we will find matching stmts.
 * @param stmts List of available stmts.
 */
export const getStatementsForTransaction = (
  txn: Transaction | null,
  stmts: Statement[] | null,
): Statement[] => {
  if (!txn || !stmts?.length) return [];

  const txnIDString = txn.stats_data?.transaction_fingerprint_id?.toString();

  return stmts.filter(
    s =>
      s.key.key_data?.transaction_fingerprint_id?.toString() === txnIDString ||
      s.txn_fingerprint_ids?.map(t => t.toString()).includes(txnIDString),
  );
};
