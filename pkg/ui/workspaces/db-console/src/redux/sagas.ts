// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, fork } from "redux-saga/effects";

import { timeScaleSaga } from "src/redux/timeScale";

import { analyticsSaga } from "./analyticsSagas";
import { customAnalyticsSaga } from "./customAnalytics";
import { indexUsageStatsSaga } from "./indexUsageStats";
import { jobsSaga } from "./jobs/jobsSagas";
import { localSettingsSaga } from "./localsettings";
import { queryMetricsSaga } from "./metrics";
import { sessionsSaga } from "./sessions";
import { sqlStatsSaga } from "./sqlStats";
import { statementsSaga } from "./statements";

export default function* rootSaga() {
  yield all([
    fork(queryMetricsSaga),
    fork(localSettingsSaga),
    fork(customAnalyticsSaga),
    fork(statementsSaga),
    fork(jobsSaga),
    fork(analyticsSaga),
    fork(sessionsSaga),
    fork(sqlStatsSaga),
    fork(indexUsageStatsSaga),
    fork(timeScaleSaga),
  ]);
}
