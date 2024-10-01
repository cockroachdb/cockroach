// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";
import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";

import { getJobs } from "src/api/jobsApi";

import {
  allJobsFixture,
  earliestRetainedTime,
} from "../../jobs/jobsPage/jobsPage.fixture";

import { actions, reducer, JobsState } from "./jobs.reducer";
import { refreshJobsSaga, requestJobsSaga } from "./jobs.sagas";

describe("jobs sagas", () => {
  const lastUpdated = moment.utc(new Date("2023-02-21T12:00:00.000Z"));

  const payload = new cockroach.server.serverpb.JobsRequest({
    limit: 0,
    type: 0,
    status: "",
  });
  const jobsResponse = new cockroach.server.serverpb.JobsResponse({
    jobs: allJobsFixture,
    earliest_retained_time: earliestRetainedTime,
  });

  const jobsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getJobs), jobsResponse],
  ];

  let spy: jest.SpyInstance;

  beforeAll(() => {
    spy = jest.spyOn(moment, "utc").mockImplementation(() => lastUpdated);
  });

  afterAll(() => {
    spy.mockRestore();
  });

  describe("refreshJobsSaga", () => {
    it("dispatches refresh jobs action", () => {
      return expectSaga(refreshJobsSaga, actions.request(payload))
        .provide(jobsAPIProvider)
        .put(actions.request(payload))
        .run();
    });
  });

  describe("requestJobsSaga", () => {
    it("successfully requests jobs", () => {
      return expectSaga(requestJobsSaga, actions.request(payload))
        .provide(jobsAPIProvider)
        .put(actions.received(jobsResponse))
        .withReducer(reducer)
        .hasFinalState<JobsState>({
          data: jobsResponse,
          error: null,
          valid: true,
          inFlight: false,
          lastUpdated,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestJobsSaga, actions.request(payload))
        .provide([[matchers.call.fn(getJobs), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<JobsState>({
          data: null,
          error: error,
          valid: false,
          inFlight: false,
          lastUpdated,
        })
        .run();
    });
  });
});
