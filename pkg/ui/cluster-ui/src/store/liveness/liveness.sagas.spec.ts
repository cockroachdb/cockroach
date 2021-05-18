import { expectSaga } from "redux-saga-test-plan";
import { throwError } from "redux-saga-test-plan/providers";
import * as matchers from "redux-saga-test-plan/matchers";
import { getLiveness } from "src/api/livenessApi";
import {
  receivedLivenessSaga,
  refreshLivenessSaga,
  requestLivenessSaga,
} from "./liveness.sagas";
import { actions, reducer, LivenessState } from "./liveness.reducer";
import { getLivenessResponse } from "./liveness.fixtures";

describe("Liveness sagas", () => {
  const livenessResponse = getLivenessResponse();

  describe("refreshLivenessSaga", () => {
    it("dispatches request for node liveness statuses", () => {
      expectSaga(refreshLivenessSaga)
        .put(actions.request())
        .run();
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
