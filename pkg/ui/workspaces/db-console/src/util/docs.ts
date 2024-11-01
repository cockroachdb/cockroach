// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { getDataFromServer } from "src/util/dataFromServer";

const stable = "stable";

function docsURL(pageName: string): string {
  const version = getDataFromServer().Version || stable;
  const docsURLBase = "https://www.cockroachlabs.com/docs/" + version;
  return `${docsURLBase}/${pageName}`;
}

function docsURLNoVersion(pageName: string): string {
  const docsURLBaseNoVersion = "https://www.cockroachlabs.com/docs/" + stable;
  return `${docsURLBaseNoVersion}/${pageName}`;
}

export let adminUILoginNoVersion = docsURLNoVersion(
  "ui-overview.html#db-console-security-considerations",
);
export let startFlags: string;
export let pauseJob: string;
export let cancelJob: string;
export let enableNodeMap: string;
export let configureReplicationZones: string;
export let transactionalPipelining: string;
export let adminUIAccess: string;
export let howAreCapacityMetricsCalculated: string;
export let howAreCapacityMetricsCalculatedOverview: string;
export let keyValuePairs: string;
export let writeIntents: string;
export let metaRanges: string;
export let databaseTable: string;
export let jobTable: string;
export let jobStatus: string;
export let jobsPause: string;
export let jobsResume: string;
export let jobsCancel: string;
export let statementsTable: string;
export let statementDiagnostics: string;
export let statementsSql: string;
export let statementsRetries: string;
export let transactionRetryErrorReference: string;
export let capacityMetrics: string;
export let nodeLivenessIssues: string;
export let howItWork: string;
export let clusterStore: string;
export let clusterGlossary: string;
export let clusterSettings: string;
export let reviewOfCockroachTerminology: string;
export let privileges: string;
export let showSessions: string;
export let sessionsTable: string;
export let upgradeTroubleshooting: string;
export let licensingFaqs: string;
export let throttlingFaqs: string;
// Note that these explicitly don't use the current version, since we want to
// link to the most up-to-date documentation available.
export const upgradeCockroachVersion =
  "https://www.cockroachlabs.com/docs/stable/upgrade-cockroach-version.html";
export const enterpriseLicensing =
  "https://www.cockroachlabs.com/docs/stable/enterprise-licensing.html";

// Not actually a docs URL.
export const startTrial = "https://www.cockroachlabs.com/pricing/start-trial/";

export let reduceStorageOfTimeSeriesDataOperationalFlags: string;
export let performanceBestPracticesHotSpots: string;
export let uiDebugPages: string;
export let readsAndWritesOverviewPage: string;

export const recomputeDocsURLs = () => {
  adminUILoginNoVersion = docsURLNoVersion(
    "ui-overview.html#db-console-security-considerations",
  );
  startFlags = docsURL("start-a-node.html#flags");
  pauseJob = docsURL("pause-job.html");
  cancelJob = docsURL("cancel-job.html");
  enableNodeMap = docsURL("enable-node-map.html");
  configureReplicationZones = docsURL("configure-replication-zones.html");
  transactionalPipelining = docsURL(
    "architecture/transaction-layer.html#transaction-pipelining",
  );
  adminUIAccess = docsURL("ui-overview.html#db-console-access");
  howAreCapacityMetricsCalculated = docsURL(
    "ui-storage-dashboard.html#capacity-metrics",
  );
  howAreCapacityMetricsCalculatedOverview = docsURL(
    "ui-cluster-overview-page.html#capacity-metrics",
  );
  keyValuePairs = docsURL("architecture/distribution-layer.html#table-data");
  writeIntents = docsURL("architecture/transaction-layer.html#write-intents");
  metaRanges = docsURL("architecture/distribution-layer.html#meta-ranges");
  databaseTable = docsURL("ui-databases-page.html");
  jobTable = docsURL("ui-jobs-page.html");
  jobStatus = docsURL("ui-jobs-page.html#job-status");
  jobsPause = docsURL("pause-job");
  jobsResume = docsURL("resume-job");
  jobsCancel = docsURL("cancel-job");
  statementsTable = docsURL("ui-statements-page.html");
  statementDiagnostics = docsURL("ui-statements-page.html#diagnostics");
  statementsSql = docsURL("ui-statements-page.html#sql-statement-fingerprints");
  statementsRetries = docsURL("transactions.html#transaction-retries");
  transactionRetryErrorReference = docsURL(
    "transaction-retry-error-reference.html",
  );
  capacityMetrics = docsURL("ui-cluster-overview-page.html#capacity-metrics");
  nodeLivenessIssues = docsURL(
    "cluster-setup-troubleshooting.html#node-liveness-issues",
  );
  howItWork = docsURL("cockroach-quit.html#how-it-works");
  clusterStore = docsURL("cockroach-start.html#store");
  clusterGlossary = docsURL("architecture/overview.html#glossary");
  clusterSettings = docsURL("cluster-settings.html");
  reviewOfCockroachTerminology = docsURL(
    "ui-replication-dashboard.html#review-of-cockroachdb-terminology",
  );
  privileges = docsURL("authorization.html#privileges");
  showSessions = docsURL("show-sessions.html");
  sessionsTable = docsURL("ui-sessions-page.html");
  reduceStorageOfTimeSeriesDataOperationalFlags = docsURL(
    "operational-faqs.html#can-i-reduce-or-disable-the-storage-of-time-series-data",
  );
  performanceBestPracticesHotSpots = docsURL(
    "performance-best-practices-overview.html#hot-spots",
  );
  uiDebugPages = docsURL("ui-debug-pages.html");
  readsAndWritesOverviewPage = docsURLNoVersion(
    "architecture/reads-and-writes-overview.html#important-concepts",
  );
  upgradeTroubleshooting = docsURL(
    "upgrade-cockroach-version.html#troubleshooting",
  );
  licensingFaqs = docsURL("licensing-faqs#renew-an-expired-license");
  throttlingFaqs = docsURL("licensing-faqs#monitor-for-license-expiry");
};

recomputeDocsURLs();
