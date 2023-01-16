// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, takeLatest } from "redux-saga/effects";
import { actions, UserSQLRolesRequest } from "./uiConfig.reducer";
import { PayloadAction } from "@reduxjs/toolkit";
import { getUserSQLRoles } from "../../api/userApi";

export function* refreshUserSQLRoles(
  action: PayloadAction<UserSQLRolesRequest>,
): any {
  try {
    const result = yield call(getUserSQLRoles, action?.payload);
    yield put(actions.refreshUserSQLRoles(result));
  } catch (e) {
    console.warn(e.message);
  }
}

export function* uiConfigSaga() {
  yield all([takeLatest(actions.refreshUserSQLRoles, refreshUserSQLRoles)]);
}
