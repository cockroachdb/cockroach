// Copyright 2018 The Cockroach Authors.
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

export const adminUILoginNoVersion = docsURLNoVersion("admin-ui-access-and-navigate.html#secure-the-admin-ui");
export const startFlags = docsURL("start-a-node.html#flags");
export const pauseJob = docsURL("pause-job.html");
export const cancelJob = docsURL("cancel-job.html");
export const enableNodeMap = docsURL("enable-node-map.html");
export const configureReplicationZones = docsURL("configure-replication-zones.html");
export const transactionalPipelining = docsURL("architecture/transaction-layer.html#transaction-pipelining");
export const adminUIAccess = docsURL("admin-ui-overview.html#admin-ui-access");
export const statementDiagnostics = docsURL("admin-ui-statements-page.html#diagnostics");
export const howAreCapacityMetricsCalculated = docsURL("admin-ui-storage-dashboard.html#capacity-metrics");
export const keyValuePairs = docsURL("architecture/distribution-layer.html#table-data");
export const databaseTable = docsURL("admin-ui-databases-page.html");
export const jobTable = docsURL("admin-ui-jobs-page.html");
export const statementsTable = docsURL("admin-ui-statements-page.html");

// Note that these explicitly don't use the current version, since we want to
// link to the most up-to-date documentation available.
export const upgradeCockroachVersion = "https://www.cockroachlabs.com/docs/stable/upgrade-cockroach-version.html";
export const enterpriseLicensing = "https://www.cockroachlabs.com/docs/stable/enterprise-licensing.html";

// Not actually a docs URL.
export const startTrial = "https://www.cockroachlabs.com/pricing/start-trial/";
