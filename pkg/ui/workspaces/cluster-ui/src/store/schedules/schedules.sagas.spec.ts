// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { expectSaga } from "redux-saga-test-plan";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";
import * as matchers from "redux-saga-test-plan/matchers";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { getSchedules } from "src/api/schedulesApi";
import {
  refreshSchedulesSaga,
  requestSchedulesSaga,
  receivedSchedulesSaga,
} from "./schedules.sagas";
import { actions, reducer, SchedulesState } from "./schedules.reducer";
import {
  allSchedulesFixture,
  earliestRetainedTime,
} from "../../schedules/schedulesPage/schedulesPage.fixture";

describe("schedules sagas", () => {
  const payload = new cockroach.server.serverpb.SchedulesRequest({
    limit: 0,
    type: 0,
    status: "",
  });
  const schedulesResponse = new cockroach.server.serverpb.SchedulesResponse({
    schedules: allSchedulesFixture,
    earliest_retained_time: earliestRetainedTime,
  });

  const schedulesAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getSchedules), schedulesResponse],
  ];

  describe("refreshSchedulesSaga", () => {
    it("dispatches refresh schedules action", () => {
      return expectSaga(refreshSchedulesSaga, actions.request(payload))
        .provide(schedulesAPIProvider)
        .put(actions.request(payload))
        .run();
    });
  });

  describe("requestSchedulesSaga", () => {
    it("successfully requests schedules", () => {
      return expectSaga(requestSchedulesSaga, actions.request(payload))
        .provide(schedulesAPIProvider)
        .put(actions.received(schedulesResponse))
        .withReducer(reducer)
        .hasFinalState<SchedulesState>({
          data: schedulesResponse,
          lastError: null,
          valid: true,
          inFlight: false,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestSchedulesSaga, actions.request(payload))
        .provide([[matchers.call.fn(getSchedules), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<SchedulesState>({
          data: null,
          lastError: error,
          valid: false,
          inFlight: false,
        })
        .run();
    });
  });

  describe("receivedSchedulesSaga", () => {
    it("sets valid status to false after specified period of time", () => {
      const timeout = 500;
      return expectSaga(receivedSchedulesSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: schedulesResponse,
          lastError: null,
          valid: true,
          inFlight: false,
        })
        .hasFinalState<SchedulesState>({
          data: schedulesResponse,
          lastError: null,
          valid: false,
          inFlight: false,
        })
        .run(1000);
    });
  });
});
