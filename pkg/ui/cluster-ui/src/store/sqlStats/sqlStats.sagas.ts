import { all, call, put, takeEvery } from "redux-saga/effects";
import { resetSQLStats } from "src/api/sqlStatsApi";
import { actions as statementActions } from "src/store/statements";
import { actions as sqlStatsActions } from "./sqlStats.reducer";

export function* resetSQLStatsSaga() {
  try {
    const response = yield call(resetSQLStats);
    yield put(sqlStatsActions.received(response));
    yield put(statementActions.invalidated());
    yield put(statementActions.refresh());
  } catch (e) {
    yield put(sqlStatsActions.failed(e));
  }
}

export function* sqlStatsSaga() {
  yield all([takeEvery(sqlStatsActions.request, resetSQLStatsSaga)]);
}
