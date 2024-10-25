// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SagaIterator } from "redux-saga";
import { all, fork } from "redux-saga/effects";

import { clusterLocksSaga } from "./clusterLocks/clusterLocks.saga";
import { clusterSettingsSaga } from "./clusterSettings/clusterSettings.saga";
import { databasesListSaga } from "./databasesList";
import { indexStatsSaga } from "./indexStats";
import { transactionInsightDetailsSaga } from "./insightDetails/transactionInsightDetails";
import { statementFingerprintInsightsSaga } from "./insights/statementFingerprintInsights";
import { statementInsightsSaga } from "./insights/statementInsights";
import { transactionInsightsSaga } from "./insights/transactionInsights/transactionInsights.sagas";
import { jobSaga } from "./jobDetails";
import { jobsSaga } from "./jobs";
import { livenessSaga } from "./liveness";
import { localStorageSaga } from "./localStorage";
import { nodesSaga } from "./nodes";
import { notifificationsSaga } from "./notifications";
import { schemaInsightsSaga } from "./schemaInsights";
import { sessionsSaga } from "./sessions";
import { sqlStatsSaga } from "./sqlStats";
import { sqlDetailsStatsSaga } from "./statementDetails";
import { statementsDiagnosticsSagas } from "./statementDiagnostics";
import { terminateSaga } from "./terminateQuery";
import { txnStatsSaga } from "./transactionStats";
import { uiConfigSaga } from "./uiConfig";

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
