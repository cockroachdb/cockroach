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

import { getJob } from "src/api/jobsApi";
import { refreshJobSaga, requestJobSaga, receivedJobSaga } from "./job.sagas";
import { actions, reducer, JobState } from "./job.reducer";
import { succeededJobFixture } from "../../jobs/jobsPage/jobsPage.fixture";
import Long from "long";

describe("job sagas", () => {
  const payload = new cockroach.server.serverpb.JobRequest({
    job_id: new Long(8136728577, 70289336),
  });
  const jobResponse = new cockroach.server.serverpb.JobResponse(
    succeededJobFixture,
  );

  const jobAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getJob), jobResponse],
  ];

  describe("refreshJobSaga", () => {
    it("dispatches refresh job action", () => {
      return expectSaga(refreshJobSaga, actions.request(payload))
        .provide(jobAPIProvider)
        .put(actions.request(payload))
        .run();
    });
  });

  describe("requestJobSaga", () => {
    it("successfully requests job", () => {
      return expectSaga(requestJobSaga, actions.request(payload))
        .provide(jobAPIProvider)
        .put(actions.received(jobResponse))
        .withReducer(reducer)
        .hasFinalState<JobState>({
          data: jobResponse,
          lastError: null,
          valid: true,
          inFlight: false,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestJobSaga, actions.request(payload))
        .provide([[matchers.call.fn(getJob), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<JobState>({
          data: null,
          lastError: error,
          valid: false,
          inFlight: false,
        })
        .run();
    });
  });

  describe("receivedJobSaga", () => {
    it("sets valid status to false after specified period of time", () => {
      const timeout = 500;
      return expectSaga(receivedJobSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: jobResponse,
          lastError: null,
          valid: true,
          inFlight: false,
        })
        .hasFinalState<JobState>({
          data: jobResponse,
          lastError: null,
          valid: false,
          inFlight: false,
        })
        .run(1000);
    });
  });
});
