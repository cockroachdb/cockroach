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

import { getSchedule } from "src/api/schedulesApi";
import {
  refreshScheduleSaga,
  requestScheduleSaga,
  receivedScheduleSaga,
} from "./schedule.sagas";
import { actions, reducer, ScheduleState } from "./schedule.reducer";
import { succeededScheduleFixture } from "../../schedules/schedulesPage/schedulesPage.fixture";
import Long from "long";

describe("schedule sagas", () => {
  const payload = new cockroach.server.serverpb.ScheduleRequest({
    schedule_id: new Long(8136728577, 70289336),
  });
  const scheduleResponse = new cockroach.server.serverpb.ScheduleResponse(
    succeededScheduleFixture,
  );

  const scheduleAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getSchedule), scheduleResponse],
  ];

  describe("refreshScheduleSaga", () => {
    it("dispatches refresh schedule action", () => {
      return expectSaga(refreshScheduleSaga, actions.request(payload))
        .provide(scheduleAPIProvider)
        .put(actions.request(payload))
        .run();
    });
  });

  describe("requestScheduleSaga", () => {
    it("successfully requests schedule", () => {
      return expectSaga(requestScheduleSaga, actions.request(payload))
        .provide(scheduleAPIProvider)
        .put(actions.received(scheduleResponse))
        .withReducer(reducer)
        .hasFinalState<ScheduleState>({
          data: scheduleResponse,
          lastError: null,
          valid: true,
          inFlight: false,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestScheduleSaga, actions.request(payload))
        .provide([[matchers.call.fn(getSchedule), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<ScheduleState>({
          data: null,
          lastError: error,
          valid: false,
          inFlight: false,
        })
        .run();
    });
  });

  describe("receivedScheduleSaga", () => {
    it("sets valid status to false after specified period of time", () => {
      const timeout = 500;
      return expectSaga(receivedScheduleSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: scheduleResponse,
          lastError: null,
          valid: true,
          inFlight: false,
        })
        .hasFinalState<ScheduleState>({
          data: scheduleResponse,
          lastError: null,
          valid: false,
          inFlight: false,
        })
        .run(1000);
    });
  });
});
