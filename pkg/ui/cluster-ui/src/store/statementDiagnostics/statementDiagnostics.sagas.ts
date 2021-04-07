import {
  all,
  call,
  delay,
  put,
  takeEvery,
  takeLatest,
} from "redux-saga/effects";
import {
  createStatementDiagnosticsReport,
  getStatementDiagnosticsReports,
} from "src/api/statementDiagnosticsApi";
import { actions } from "./statementDiagnostics.reducer";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import { rootActions } from "../reducers";

export function* createDiagnosticsReportSaga(
  action: ReturnType<typeof actions.createReport>,
) {
  try {
    yield call(createStatementDiagnosticsReport, action.payload);
    yield put(actions.createReportCompleted());
    // request diagnostics reports to reflect changed state for newly
    // requested statement.
    yield put(actions.request());
  } catch (e) {
    yield put(actions.createReportFailed(e));
  }
}

export function* refreshStatementsDiagnosticsSaga() {
  yield put(actions.request());
}

export function* requestStatementsDiagnosticsSaga() {
  try {
    const response = yield call(getStatementDiagnosticsReports);
    yield put(actions.received(response));
  } catch (e) {
    yield put(actions.failed(e));
  }
}

export function* receivedStatementsDiagnosticsSaga(delayMs: number) {
  yield delay(delayMs);
  yield put(actions.invalidated());
}

export function* statementsDiagnosticsSagas(
  delayMs: number = CACHE_INVALIDATION_PERIOD,
) {
  yield all([
    throttleWithReset(
      delayMs,
      actions.refresh,
      [actions.invalidated, actions.failed, rootActions.resetState],
      refreshStatementsDiagnosticsSaga,
    ),
    takeLatest(actions.request, requestStatementsDiagnosticsSaga),
    takeEvery(actions.createReport, createDiagnosticsReportSaga),
    takeLatest(actions.received, receivedStatementsDiagnosticsSaga, delayMs),
  ]);
}
