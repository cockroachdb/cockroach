// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { all, call, delay, put, takeLatest } from "redux-saga/effects";
import { actions } from "./uiConfig.reducer";
import { getUserSQLRoles } from "../../api/userApi";
import { CACHE_INVALIDATION_PERIOD, throttleWithReset } from "../utils";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { getLogger } from "../../util";
import { rootActions } from "../rootActions";

export function* refreshUserSQLRolesSaga(): any {
  yield put(actions.requestUserSQLRoles());
}

export function* requestUserSQLRolesSaga(): any {
  try {
    const result: cockroach.server.serverpb.UserSQLRolesResponse = yield call(
      getUserSQLRoles,
    );
    yield put(actions.receivedUserSQLRoles(result.roles));
  } catch (e) {
    getLogger().warn(e.message, /* additional context */ undefined, e);
  }
}

export function* receivedUserSQLRolesSaga(delayMs: number): any {
  yield delay(delayMs);
  yield put(actions.invalidatedUserSQLRoles());
}

export function* uiConfigSaga(
  cacheInvalidationPeriod: number = CACHE_INVALIDATION_PERIOD,
): any {
  yield all([
    throttleWithReset(
      cacheInvalidationPeriod,
      actions.refreshUserSQLRoles,
      [actions.invalidatedUserSQLRoles, rootActions.resetState],
      refreshUserSQLRolesSaga,
    ),
    takeLatest(actions.requestUserSQLRoles, requestUserSQLRolesSaga),
    takeLatest(
      actions.receivedUserSQLRoles,
      receivedUserSQLRolesSaga,
      cacheInvalidationPeriod,
    ),
  ]);
}
