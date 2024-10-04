// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, call, put, takeEvery } from "redux-saga/effects";

import { actions } from "./tableDetails.reducer";
import { ErrorWithKey, getTableDetails, TableDetailsReqParams } from "src/api";
import moment from "moment";
import { PayloadAction } from "@reduxjs/toolkit";
import { generateTableID } from "../../util";

export function* refreshTableDetailsSaga(
  action: PayloadAction<TableDetailsReqParams>,
) {
  yield put(actions.request(action.payload));
}

export function* requestTableDetailsSaga(
  action: PayloadAction<TableDetailsReqParams>,
): any {
  const key = generateTableID(action.payload.database, action.payload.table);
  try {
    const result = yield call(
      getTableDetails,
      action.payload,
      moment.duration(10, "m"),
    );
    yield put(actions.received({ key, tableDetailsResponse: result }));
  } catch (e) {
    const err: ErrorWithKey = {
      err: e,
      key,
    };
    yield put(actions.failed(err));
  }
}

export function* tableDetailsSaga() {
  yield all([
    takeEvery(actions.refresh, refreshTableDetailsSaga),
    takeEvery(actions.request, requestTableDetailsSaga),
  ]);
}
