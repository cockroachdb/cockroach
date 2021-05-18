// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AnyAction } from "redux";
import { all, call, takeEvery } from "redux-saga/effects";
import { actions } from "./localStorage.reducer";

export function* updateLocalStorageItemSaga(action: AnyAction) {
  const { key, value } = action.payload;
  yield call({ context: localStorage, fn: localStorage.setItem }, key, value);
}

export function* localStorageSaga() {
  yield all([takeEvery(actions.update, updateLocalStorageItemSaga)]);
}
