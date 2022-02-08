// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { getDataFromServer } from "src/util/dataFromServer";

const stable = "stable";
const version = getDataFromServer().Version || stable;
const docsURLBase = "https://www.cockroachlabs.com/docs/" + version;
const docsURLBaseNoVersion = "https://www.cockroachlabs.com/docs/" + stable;

function docsURL(pageName: string): string {
  return `${docsURLBase}/${pageName}`;
}

function docsURLNoVersion(pageName: string): string {
  return `${docsURLBaseNoVersion}/${pageName}`;
}

export const adminUILoginNoVersion = docsURLNoVersion(
  "ui-overview.html#db-console-access",
);
export const startFlags = docsURL("start-a-node.html#flags");
export const pauseJob = docsURL("pause-job.html");
export const cancelJob = docsURL("cancel-job.html");
export const enableNodeMap = docsURL("enable-node-map.html");
export const configureReplicationZones = docsURL(
  "configure-replication-zones.html",
);
export const transactionalPipelining = docsURL(
  "architecture/transaction-layer.html#transaction-pipelining",
);
export const adminUIAccess = docsURL("ui-overview.html#db-console-access");
export const howAreCapacityMetricsCalculated = docsURL(
  "ui-storage-dashboard.html#capacity-metrics",
);
export const keyValuePairs = docsURL(
  "architecture/distribution-layer.html#table-data",
);
export const databaseTable = docsURL("ui-databases-page.html");
export const jobTable = docsURL("ui-jobs-page.html");
export const statementsTable = docsURL("ui-statements-page.html");
export const statementDiagnostics = docsURL(
  "ui-statements-page.html#diagnostics",
);
export const statementsSql = docsURL(
  "ui-statements-page.html#sql-statement-fingerprints",
);
export const statementsRetries = docsURL(
  "transactions.html#transaction-retries",
);
export const statementsTimeInterval = docsURL(
  "ui-statements-page.html#time-interval",
);
export const readFromDisk = docsURL(
  "architecture/life-of-a-distributed-transaction.html#reads-from-the-storage-layer",
);
export const writtenToDisk = docsURL(
  "architecture/life-of-a-distributed-transaction.html#write-operations",
);
export const planningExecutionTime = docsURL(
  "architecture/sql-layer#sql-parser-planner-executor",
);
export const contentionTime = docsURL(
  "performance-best-practices-overview#understanding-and-avoiding-transaction-contention",
);
export const readsAndWrites = docsURL("architecture/reads-and-writes-overview");
export const capacityMetrics = docsURL(
  "ui-cluster-overview-page.html#capacity-metrics",
);
export const nodeLivenessIssues = docsURL(
  "cluster-setup-troubleshooting.html#node-liveness-issues",
);
export const howItWork = docsURL("cockroach-quit.html#how-it-works");
export const clusterStore = docsURL("cockroach-start.html#store");
export const clusterGlossary = docsURL("architecture/overview.html#glossary");
export const reviewOfCockroachTerminology = docsURL(
  "ui-replication-dashboard.html#review-of-cockroachdb-terminology",
);
export const sessionsTable = docsURL("ui-sessions-page.html");
export const tableStatsClusterSetting = docsURL(
  "cost-based-optimizer.html#control-automatic-statistics",
);
// Note that these explicitly don't use the current version, since we want to
// link to the most up-to-date documentation available.
export const upgradeCockroachVersion =
  "https://www.cockroachlabs.com/docs/stable/upgrade-cockroach-version.html";
export const enterpriseLicensing =
  "https://www.cockroachlabs.com/docs/stable/enterprise-licensing.html";

// Explain plan
export const distSql = docsURL("architecture/sql-layer.html#distsql");
export const vectorizedExecution = docsURL(
  "vectorized-execution.html#configuring-vectorized-execution",
);
export const mergeJoin = docsURL("joins.html#merge-joins");
export const lookupJoin = docsURL("joins.html#lookup-joins");
export const hashJoin = docsURL("joins.html#hash-joins");
export const invertedJoin = docsURL("joins.html#inverted-joins");
export const indexJoin = docsURL("indexes.html#storing-columns");
export const fullScan = docsURL(
  "sql-tuning-with-explain.html#issue-full-table-scans",
);
export const secondaryIndex = docsURL("schema-design-indexes.html");
export const lockingStrength = docsURL(
  "explain.html#find-out-if-a-statement-is-using-select-for-update-locking",
);
export const transactionLayerOverview = docsURL(
  "architecture/transaction-layer.html#overview",
);

// Not actually a docs URL.
export const startTrial = "https://www.cockroachlabs.com/pricing/start-trial/";
export const transactionsTable = docsURL("ui-transactions-page.html");
