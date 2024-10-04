// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, fork } from "redux-saga/effects";

import { queryMetricsSaga } from "./metrics";
import { localSettingsSaga } from "./localsettings";
import { customAnalyticsSaga } from "./customAnalytics";
import { statementsSaga } from "./statements";
import { analyticsSaga } from "./analyticsSagas";
import { sessionsSaga } from "./sessions";
import { sqlStatsSaga } from "./sqlStats";
import { indexUsageStatsSaga } from "./indexUsageStats";
import { timeScaleSaga } from "src/redux/timeScale";
import { jobsSaga } from "./jobs/jobsSagas";

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
