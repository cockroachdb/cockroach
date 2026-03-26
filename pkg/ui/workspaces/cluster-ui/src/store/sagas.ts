// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SagaIterator } from "redux-saga";
import { all, fork } from "redux-saga/effects";

import { clusterLocksSaga } from "./clusterLocks/clusterLocks.saga";
import { clusterSettingsSaga } from "./clusterSettings/clusterSettings.saga";
import { databasesListSaga } from "./databasesList";
import { statementFingerprintInsightsSaga } from "./insights/statementFingerprintInsights";
import { transactionInsightsSaga } from "./insights/transactionInsights/transactionInsights.sagas";
import { livenessSaga } from "./liveness";
import { localStorageSaga } from "./localStorage";
import { nodesSaga } from "./nodes";
import { sqlStatsSaga } from "./sqlStats";
import { sqlDetailsStatsSaga } from "./statementDetails";
import { statementsDiagnosticsSagas } from "./statementDiagnostics";
import { txnStatsSaga } from "./transactionStats";
import { uiConfigSaga } from "./uiConfig";

export function* sagas(cacheInvalidationPeriod?: number): SagaIterator {
  yield all([
    fork(localStorageSaga),
    fork(statementsDiagnosticsSagas, cacheInvalidationPeriod),
    fork(nodesSaga, cacheInvalidationPeriod),
    fork(livenessSaga, cacheInvalidationPeriod),
    fork(transactionInsightsSaga),
    fork(databasesListSaga),
    fork(sqlStatsSaga),
    fork(sqlDetailsStatsSaga),
    fork(clusterLocksSaga),
    fork(uiConfigSaga, cacheInvalidationPeriod),
    fork(statementFingerprintInsightsSaga),
    fork(txnStatsSaga),
    fork(clusterSettingsSaga),
  ]);
}
