import { all, call, put, delay, takeLatest } from "redux-saga/effects";
import { getNodes } from "src/api/nodesApi";
import { actions } from "./nodes.reducer";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { accumulateMetrics } from "../../util";

export function* refreshNodesSaga() {
  yield put(actions.request());
}

export function* requestNodesSaga() {
  try {
    const result: cockroach.server.serverpb.NodesResponse = yield call(
      getNodes,
    );
    // TODO (koorosh): Would it be safe to move node transformation to selectors and
    // preserve node response "as is" in store?
    const nodes = accumulateMetrics(result.nodes);
    yield put(actions.received(nodes));
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
      [actions.invalidated, actions.failed],
      refreshNodesSaga,
    ),
    takeLatest(actions.request, requestNodesSaga),
    takeLatest(actions.received, receivedNodesSaga, cacheInvalidationPeriod),
  ]);
}
