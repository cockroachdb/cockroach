// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import { throwError } from "redux-saga-test-plan/providers";

import { getLiveness } from "src/api/livenessApi";

import { getLivenessResponse } from "./liveness.fixtures";
import { actions, reducer, LivenessState } from "./liveness.reducer";
import {
  receivedLivenessSaga,
  refreshLivenessSaga,
  requestLivenessSaga,
} from "./liveness.sagas";

describe("Liveness sagas", () => {
  const livenessResponse = getLivenessResponse();

  describe("refreshLivenessSaga", () => {
    it("dispatches request for node liveness statuses", () => {
      expectSaga(refreshLivenessSaga).put(actions.request()).run();
    });
  });

  describe("requestLivenessSaga", () => {
    it("successfully requests node liveness statuses", () => {
      expectSaga(requestLivenessSaga)
        .provide([[matchers.call.fn(getLiveness), livenessResponse]])
        .put(actions.received(livenessResponse))
        .withReducer(reducer)
        .hasFinalState<LivenessState>({
          data: livenessResponse,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      expectSaga(requestLivenessSaga)
        .provide([[matchers.call.fn(getLiveness), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<LivenessState>({
          data: null,
          lastError: error,
          valid: false,
        })
        .run();
    });
  });

  describe("receivedLivenessSaga", () => {
    it("sets valid status to false after specified period of time", () => {
      const timeout = 500;
      expectSaga(receivedLivenessSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: livenessResponse,
          lastError: null,
          valid: true,
        })
        .hasFinalState<LivenessState>({
          data: livenessResponse,
          lastError: null,
          valid: false,
        })
        .run(1000);
    });
  });
});
