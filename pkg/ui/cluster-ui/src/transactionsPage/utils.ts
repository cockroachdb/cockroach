import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { Filters } from "./";
import { SelectOptions } from "./filter";
import { AggregateStatistics } from "../statementsTable";
import Long from "long";
import _ from "lodash";
import { addExecStats, aggregateNumericStats, FixLong } from "../util";

type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;
type Transaction = protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
type ExecStats = protos.cockroach.sql.IExecStats;

export const getTrxAppFilterOptions = (
  transactions: Transaction[],
  prefix: string,
): SelectOptions[] => {
  const defaultAppFilters = ["All", prefix];
  const uniqueAppNames = new Set(
    transactions
      .filter(t => !t.stats_data.app.startsWith(prefix))
      .map(t => t.stats_data.app),
  );

  return defaultAppFilters
    .concat(Array.from(uniqueAppNames))
    .map(filterValue => ({
      label: filterValue,
      value: filterValue,
    }));
};

export const collectStatementsText = (statements: Statement[]): string =>
  statements.map(s => s.key.key_data.query).join("\n");

export const getStatementsById = (
  statementsIds: Long[],
  statements: Statement[],
): Statement[] => {
  return statements.filter(s => statementsIds.some(id => id.eq(s.id)));
};

export const aggregateStatements = (
  statements: Statement[],
): AggregateStatistics[] =>
  statements.map((s: Statement) => ({
    label: s.key.key_data.query,
    implicitTxn: false,
    fullScan: s.key.key_data.full_scan,
    stats: s.stats,
  }));

export const searchTransactionsData = (
  search: string,
  transactions: Transaction[],
  statements: Statement[],
): Transaction[] => {
  return transactions.filter((t: Transaction) =>
    search.split(" ").every(val =>
      collectStatementsText(
        getStatementsById(t.stats_data.statement_ids, statements),
      )
        .toLowerCase()
        .includes(val.toLowerCase()),
    ),
  );
};

function getTimeValue(timeNumber: string, timeUnit: string): number | "empty" {
  if (arguments.length < 2 || timeNumber === "0") return "empty";
  return timeUnit === "seconds"
    ? Number(timeNumber)
    : Number(timeNumber) / 1000;
}

export const filterTransactions = (
  data: Transaction[],
  filters: Filters,
  internalAppNamePrefix: string,
): { transactions: Transaction[]; activeFilters: number } => {
  if (!filters)
    return {
      transactions: data,
      activeFilters: 0,
    };
  const { timeNumber, timeUnit } = filters;
  const timeValue = getTimeValue(timeNumber, timeUnit);
  const filtersStatus = [
    timeValue && timeValue !== "empty",
    filters.app !== "All",
  ];
  const activeFilters = filtersStatus.filter(f => f).length;

  const filteredTransactions = data.filter((t: Transaction) => {
    const matchAppNameExactly = t.stats_data.app === filters.app;
    const filterIsSetToAll = filters.app === "All";
    const filterIsInternalMatchByPrefix =
      filters.app === internalAppNamePrefix &&
      t.stats_data.app.includes(filters.app);
    const validateTransaction = [
      matchAppNameExactly || filterIsSetToAll || filterIsInternalMatchByPrefix,
      t.stats_data.stats.service_lat.mean >= timeValue || timeValue === "empty",
    ];
    return validateTransaction.every(f => f);
  });

  return {
    transactions: filteredTransactions,
    activeFilters,
  };
};

type TransactionWithFingerprint = Transaction & { fingerprint: string };

// withFingerprint adds the concatenated statement fingerprints to the Transaction object since it
// only comes with statement_ids
const withFingerprint = function(
  t: Transaction,
  stmts: Statement[],
): TransactionWithFingerprint {
  return {
    ...t,
    fingerprint: collectStatementsText(
      getStatementsById(t.stats_data.statement_ids, stmts),
    ),
  };
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
    rows_read: aggregateNumericStats(a.rows_read, b.rows_read, countA, countB),
    bytes_read: aggregateNumericStats(
      a.bytes_read,
      b.bytes_read,
      countA,
      countB,
    ),
    exec_stats: addExecStats(a.exec_stats, b.exec_stats),
  };
}

function combineTransactionStats(
  txnStats: TransactionStats[],
): TransactionStats {
  return _.reduce(txnStats, addTransactionStats);
}

// mergeTransactionStats takes a list of transactions (assuming they're all for the same fingerprint
// and returns a copy of the first element with its `stats_data.stats` object replaced with a
// merged stats object that aggregates statistics from every copy of the fingerprint in the list
// provided
const mergeTransactionStats = function(txns: Transaction[]): Transaction {
  if (txns.length === 0) {
    return null;
  }
  const txn = { ...txns[0] };
  txn.stats_data.stats = combineTransactionStats(
    txns.map(t => t.stats_data.stats),
  );
  return txn;
};

// aggregateAcrossNodeIDs takes a list of transactions and a list of statemenst that those
// transactions reference and returns a list of transactions that have been grouped by their
// fingerprints and had their statistics aggregated across copies of the transaction. This is used
// to deduplicate identical copies of the transaction that are run on different nodes. CRDB returns
// different objects to represent those transactions.
//
// The function uses the fingerprint and the `app` that ran the transaction as the key to group the
// transactions when deduping.
//
export const aggregateAcrossNodeIDs = function(
  t: Transaction[],
  stmts: Statement[],
): Transaction[] {
  return _.chain(t)
    .map(t => withFingerprint(t, stmts))
    .groupBy(t => t.fingerprint + t.stats_data.app)
    .mapValues(mergeTransactionStats)
    .values()
    .value();
};
