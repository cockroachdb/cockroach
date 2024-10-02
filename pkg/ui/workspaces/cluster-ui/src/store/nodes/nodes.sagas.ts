// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { all, call, put, delay, takeLatest } from "redux-saga/effects";

import { getNodes } from "src/api/nodesApi";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";

import { maybeError } from "../../util";
import { rootActions } from "../rootActions";

import { actions } from "./nodes.reducer";

export function* refreshNodesSaga() {
  yield put(actions.request());
}

export function* requestNodesSaga() {
  try {
    const result: cockroach.server.serverpb.NodesResponse =
      yield call(getNodes);
    yield put(actions.received(result.nodes));
  } catch (e) {
    yield put(actions.failed(maybeError(e)));
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
      [actions.invalidated, rootActions.resetState],
      refreshNodesSaga,
    ),
    takeLatest(actions.request, requestNodesSaga),
    takeLatest(actions.received, receivedNodesSaga, cacheInvalidationPeriod),
  ]);
}
