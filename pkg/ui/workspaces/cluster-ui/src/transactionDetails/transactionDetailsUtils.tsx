// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { createSelector } from "@reduxjs/toolkit";
import { Location } from "history";
import cloneDeep from "lodash/cloneDeep";
import { match } from "react-router";

import { SqlStatsResponse } from "../api";
import { statementFingerprintIdsToText } from "../transactionsPage/utils";
import {
  addExecStats,
  aggregateNumericStats,
  FixLong,
  getMatchParamByName,
  txnFingerprintIdAttr,
  unset,
} from "../util";

type Transaction =
  cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

type Statement =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

type TransactionStats = cockroach.sql.ITransactionStatistics;

/*
 * getTxnFromSqlStatsTxns aggregates txn stats from the provided list which match the
 * specified txn fingerprint ID and contains one of the provided app names.
 * Note that the returned txn will have other properties matching the first txn
 * in the list matching the provided criteria, but its stats_data.stats will be
 * aggregated from all matching txns.
 *
 * @param txns List of txns to search through.
 * @param txnFingerprintID Txn fingerprint ID to search for.
 * @param apps list of app names to filter by.
 */
export const getTxnFromSqlStatsTxns = (
  txns: Transaction[] | null,
  txnFingerprintID: string | null,
  apps: string[] | null,
): Transaction | null => {
  if (!txns?.length || !txnFingerprintID) {
    return null;
  }

  return mergeTransactionStats(
    txns.filter(
      txn =>
        txn.stats_data.transaction_fingerprint_id.toString() ===
          txnFingerprintID &&
        (!apps?.length || apps.includes(txn.stats_data.app || unset)),
    ),
  );
};

export const getTxnFromSqlStatsMemoized = createSelector(
  (resp: SqlStatsResponse, _match: match, _apps: string): Transaction[] =>
    resp?.transactions ?? [],
  (_resp, _match, apps): string[] =>
    apps?.split(",").map((s: string) => s.trim()) ?? [],
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
 * matching and app name matching that of the provided txn.
 * @param txnFingerprintIDString Txn fingerprint id for which we will find matching stmts.
 * @param apps List of app names to filter stmts by.
 * @param stmts List of available stmts.
 */
export const getStatementsForTransaction = (
  txnFingerprintIDString: string,
  apps: string[],
  stmts: Statement[] | null,
): Statement[] => {
  if (!txnFingerprintIDString || !stmts?.length) return [];

  return stmts.filter(
    s =>
      (s.key.key_data?.transaction_fingerprint_id?.toString() ===
        txnFingerprintIDString ||
        s.txn_fingerprint_ids
          ?.map(t => t.toString())
          .includes(txnFingerprintIDString)) &&
      (!apps?.length ||
        apps.includes(s.key.key_data.app ? s.key.key_data.app : unset)),
  );
};

// addTransactionStats adds together two stat objects into one using their counts to compute a new
// average for the numeric statistics. It's modeled after the similar `addStatementStats` function
function addTransactionStats(
  a: TransactionStats,
  b: TransactionStats,
): Required<TransactionStats> {
  const countA = FixLong(a.count).toInt();
  const countB = FixLong(b.count).toInt();
  return {
    count: a.count.add(b.count),
    max_retries: a.max_retries.greaterThan(b.max_retries)
      ? a.max_retries
      : b.max_retries,
    num_rows: aggregateNumericStats(a.num_rows, b.num_rows, countA, countB),
    service_lat: aggregateNumericStats(
      a.service_lat,
      b.service_lat,
      countA,
      countB,
    ),
    retry_lat: aggregateNumericStats(a.retry_lat, b.retry_lat, countA, countB),
    commit_lat: aggregateNumericStats(
      a.commit_lat,
      b.commit_lat,
      countA,
      countB,
    ),
    idle_lat: aggregateNumericStats(a.idle_lat, b.idle_lat, countA, countB),
    rows_read: aggregateNumericStats(a.rows_read, b.rows_read, countA, countB),
    rows_written: aggregateNumericStats(
      a.rows_written,
      b.rows_written,
      countA,
      countB,
    ),
    bytes_read: aggregateNumericStats(
      a.bytes_read,
      b.bytes_read,
      countA,
      countB,
    ),
    exec_stats: addExecStats(a.exec_stats, b.exec_stats),
  };
}

// mergeTransactionStats takes a list of transactions (assuming they're all for the same fingerprint)
// and returns a copy of the first element with its `stats_data.stats` object replaced with a
// merged stats object that aggregates statistics from every copy of the fingerprint in the list
// provided. This function SHOULD NOT mutate any objects in the provided txns array.
const mergeTransactionStats = function (txns: Transaction[]): Transaction {
  if (txns.length === 0) {
    return null;
  }

  if (txns.length === 1) {
    return txns[0];
  }

  const txn = cloneDeep(txns[0]);

  txn.stats_data.stats = txns
    .map(t => t.stats_data.stats)
    .reduce(addTransactionStats);

  return txn;
};
