// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SagaIterator } from "redux-saga";
import { all, fork } from "redux-saga/effects";

import { localStorageSaga } from "./localStorage";
import { sqlStatsSaga } from "./sqlStats";
import { uiConfigSaga } from "./uiConfig";

export function* sagas(cacheInvalidationPeriod?: number): SagaIterator {
  yield all([
    fork(localStorageSaga),
    fork(sqlStatsSaga),
    fork(uiConfigSaga, cacheInvalidationPeriod),
  ]);
}
