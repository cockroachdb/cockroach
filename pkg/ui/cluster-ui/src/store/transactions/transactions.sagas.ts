import { all, call, put, delay, takeLatest } from "redux-saga/effects";
import { getStatements } from "src/api/statementsApi";
import { actions } from "./transactions.reducer";
import { rootActions } from "../reducers";

import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "src/store/utils";

export function* refreshTransactionssSaga() {
  yield put(actions.request());
}

export function* requestTransactionsSaga() {
  try {
    const result = yield call(getStatements);
    yield put(actions.received(result));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receicedTransactionsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* transactionsSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refresh,
      [actions.invalidated, actions.failed, rootActions.resetState],
      refreshTransactionssSaga,
    ),
    takeLatest(actions.request, requestTransactionsSaga),
    takeLatest(
      actions.received,
      receicedTransactionsSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
