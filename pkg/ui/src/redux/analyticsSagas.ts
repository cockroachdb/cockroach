// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, takeEvery } from "redux-saga/effects";
import { PayloadAction } from "src/interfaces/action";
import {
  CREATE_STATEMENT_DIAGNOSTICS_REPORT,
  DiagnosticsReportPayload,
  OPEN_STATEMENT_DIAGNOSTICS_MODAL,
} from "src/redux/statements";
import {
  trackActivateDiagnostics,
  trackDiagnosticsModalOpen,
  trackPaginate,
  trackSearch,
  trackTableSort,
  trackDownloadDiagnosticsBundle,
  trackSubnavSelection,
} from "src/util/analytics";
import {
  TRACK_STATEMENTS_SEARCH,
  TRACK_STATEMENTS_PAGINATION,
  TRACK_TABLE_SORT,
  TableSortActionPayload,
  TRACK_DOWNLOAD_DIAGNOSTIC_BUNDLE,
  TRACK_STATEMENT_DETAILS_SUBNAV_SELECTION,
} from "./analyticsActions";

export function* trackActivateStatementsDiagnostics(
  action: PayloadAction<DiagnosticsReportPayload>,
) {
  const { statementFingerprint } = action.payload;
  yield call(trackActivateDiagnostics, statementFingerprint);
}

export function* trackOpenStatementsDiagnostics(
  action: PayloadAction<DiagnosticsReportPayload>,
) {
  const { statementFingerprint } = action.payload;
  yield call(trackDiagnosticsModalOpen, statementFingerprint);
}

export function* trackStatementsSearch(action: PayloadAction<number>) {
  yield call(trackSearch, action.payload);
}

export function* trackStatementsPagination(action: PayloadAction<number>) {
  yield call(trackPaginate, action.payload);
}

export function* trackTableSortChange(
  action: PayloadAction<TableSortActionPayload>,
) {
  const { tableName, columnName, ascending } = action.payload;
  yield call(trackTableSort, tableName, columnName, ascending);
}

export function* trackDownloadDiagnosticBundleSaga(
  action: PayloadAction<string>,
) {
  yield call(trackDownloadDiagnosticsBundle, action.payload);
}

export function* trackStatementDetailsSubnavSelectionSaga(
  action: PayloadAction<string>,
) {
  yield call(trackSubnavSelection, action.payload);
}

export function* analyticsSaga() {
  yield all([
    takeEvery(
      CREATE_STATEMENT_DIAGNOSTICS_REPORT,
      trackActivateStatementsDiagnostics,
    ),
    takeEvery(OPEN_STATEMENT_DIAGNOSTICS_MODAL, trackOpenStatementsDiagnostics),
    takeEvery(TRACK_STATEMENTS_SEARCH, trackStatementsSearch),
    takeEvery(TRACK_STATEMENTS_PAGINATION, trackStatementsPagination),
    takeEvery(TRACK_TABLE_SORT, trackTableSortChange),
    takeEvery(
      TRACK_DOWNLOAD_DIAGNOSTIC_BUNDLE,
      trackDownloadDiagnosticBundleSaga,
    ),
    takeEvery(
      TRACK_STATEMENT_DETAILS_SUBNAV_SELECTION,
      trackStatementDetailsSubnavSelectionSaga,
    ),
  ]);
}
