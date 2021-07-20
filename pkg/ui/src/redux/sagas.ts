// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, fork } from "redux-saga/effects";

import { queryMetricsSaga } from "./metrics";
import { localSettingsSaga } from "./localsettings";
import { customAnalyticsSaga } from "./customAnalytics";
import { statementsSaga } from "./statements";
import { analyticsSaga } from "./analyticsSagas";
import { sessionsSaga } from "./sessions";
import { sqlStatsSaga } from "./sqlStats";

export default function* rootSaga() {
  yield all([
    fork(queryMetricsSaga),
    fork(localSettingsSaga),
    fork(customAnalyticsSaga),
    fork(statementsSaga),
    fork(analyticsSaga),
    fork(sessionsSaga),
    fork(sqlStatsSaga),
  ]);
}
