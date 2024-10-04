// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
    takeEvery(terminateQueryActions.terminateSessionCompleted, function* () {
      yield put(
        notificationAction(NotificationType.Success, "Session cancelled."),
      );
    }),

    takeEvery(terminateQueryActions.terminateSessionFailed, function* () {
      yield put(
        notificationAction(
          NotificationType.Error,
          "There was an error cancelling the session",
        ),
      );
    }),

    takeEvery(terminateQueryActions.terminateQueryCompleted, function* () {
      yield put(
        notificationAction(NotificationType.Success, "Statement cancelled."),
      );
    }),

    takeEvery(terminateQueryActions.terminateQueryFailed, function* () {
      yield put(
        notificationAction(
          NotificationType.Error,
          "There was an error cancelling the statement.",
        ),
      );
    }),
  ]);
}
