// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, fork } from "redux-saga/effects";

import { timeScaleSaga } from "src/redux/timeScale";

import { indexUsageStatsSaga } from "./indexUsageStats";
import { localSettingsSaga } from "./localsettings";
import { sqlStatsSaga } from "./sqlStats";
import { statementsSaga } from "./statements";

export default function* rootSaga() {
  yield all([
    fork(localSettingsSaga),
    fork(statementsSaga),
    fork(sqlStatsSaga),
    fork(indexUsageStatsSaga),
    fork(timeScaleSaga),
  ]);
}
