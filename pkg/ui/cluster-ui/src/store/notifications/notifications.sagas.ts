// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createAction } from "@reduxjs/toolkit";
import { all, put, takeEvery } from "redux-saga/effects";

import { actions as terminateQueryActions } from "src/store/terminateQuery/terminateQuery.reducer";

export const notificationAction = createAction(
  "adminUI/notification",
  (type: NotificationType, text: string) => ({
    payload: {
      type,
      text,
    },
  }),
);

export enum NotificationType {
  Success = "success",
  Error = "error",
}

export type SendNotification = (
  type: NotificationType,
  message: string,
) => void;

export function* notifificationsSaga() {
  // ***************************** //
  // Terminate Query notifications //
  // ***************************** //
  yield all([
    takeEvery(terminateQueryActions.terminateSessionCompleted, function*() {
      yield put(
        notificationAction(NotificationType.Success, "Session terminated."),
      );
    }),

    takeEvery(terminateQueryActions.terminateSessionFailed, function*() {
      yield put(
        notificationAction(
          NotificationType.Error,
          "There was an error terminating the session",
        ),
      );
    }),

    takeEvery(terminateQueryActions.terminateQueryCompleted, function*() {
      yield put(
        notificationAction(NotificationType.Success, "Query terminated."),
      );
    }),

    takeEvery(terminateQueryActions.terminateQueryFailed, function*() {
      yield put(
        notificationAction(
          NotificationType.Error,
          "There was an error terminating the query.",
        ),
      );
    }),
  ]);
}
