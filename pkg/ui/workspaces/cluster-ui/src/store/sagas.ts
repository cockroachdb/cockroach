// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SagaIterator } from "redux-saga";
import { all, fork } from "redux-saga/effects";

import { localStorageSaga } from "./localStorage";
import { statementsDiagnosticsSagas } from "./statementDiagnostics";
import { nodesSaga } from "./nodes";
import { jobsSaga } from "./jobs";
import { jobSaga } from "./jobDetails";
import { livenessSaga } from "./liveness";
import { databasesListSaga } from "./databasesList";
import { sessionsSaga } from "./sessions";
import { terminateSaga } from "./terminateQuery";
import { notifificationsSaga } from "./notifications";
import { sqlStatsSaga } from "./sqlStats";
import { sqlDetailsStatsSaga } from "./statementDetails";
import { indexStatsSaga } from "./indexStats";
import { clusterLocksSaga } from "./clusterLocks/clusterLocks.saga";
import { transactionInsightsSaga } from "./insights/transactionInsights/transactionInsights.sagas";
import { transactionInsightDetailsSaga } from "./insightDetails/transactionInsightDetails";
import { statementInsightsSaga } from "./insights/statementInsights";
import { schemaInsightsSaga } from "./schemaInsights";
import { uiConfigSaga } from "./uiConfig";
import { statementFingerprintInsightsSaga } from "./insights/statementFingerprintInsights";
import { txnStatsSaga } from "./transactionStats";
import { clusterSettingsSaga } from "./clusterSettings/clusterSettings.saga";
import { databaseDetailsSaga } from "./databaseDetails";
import { tableDetailsSaga } from "./databaseTableDetails";
import { databaseDetailsSpanStatsSaga } from "./databaseDetails/databaseDetailsSpanStats.saga";

export function* sagas(cacheInvalidationPeriod?: number): SagaIterator {
  yield all([
    fork(localStorageSaga),
    fork(statementsDiagnosticsSagas, cacheInvalidationPeriod),
    fork(nodesSaga, cacheInvalidationPeriod),
    fork(livenessSaga, cacheInvalidationPeriod),
    fork(transactionInsightsSaga),
    fork(transactionInsightDetailsSaga),
    fork(statementInsightsSaga),
    fork(jobsSaga),
    fork(jobSaga),
    fork(databasesListSaga),
    fork(databaseDetailsSaga),
    fork(databaseDetailsSpanStatsSaga),
    fork(tableDetailsSaga),
    fork(sessionsSaga),
    fork(terminateSaga),
    fork(notifificationsSaga),
    fork(sqlStatsSaga),
    fork(sqlDetailsStatsSaga),
    fork(indexStatsSaga),
    fork(clusterLocksSaga),
    fork(schemaInsightsSaga),
    fork(uiConfigSaga, cacheInvalidationPeriod),
    fork(statementFingerprintInsightsSaga),
    fork(txnStatsSaga),
    fork(clusterSettingsSaga),
  ]);
}
