// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, delay, takeLatest } from "redux-saga/effects";
import { getNodes } from "src/api/nodesApi";
import { actions } from "./nodes.reducer";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { rootActions } from "../reducers";

export function* refreshNodesSaga() {
  yield put(actions.request());
}

export function* requestNodesSaga() {
  try {
    const result: cockroach.server.serverpb.NodesResponse = yield call(
      getNodes,
    );
    yield put(actions.received(result.nodes));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedNodesSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* nodesSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, actions.failed, rootActions.resetState],
      refreshNodesSaga,
    ),
    takeLatest(actions.request, requestNodesSaga),
    takeLatest(actions.received, receivedNodesSaga, cacheInvalidationPeriod),
  ]);
}
