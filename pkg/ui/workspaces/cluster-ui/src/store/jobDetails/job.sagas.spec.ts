// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { PayloadAction } from "@reduxjs/toolkit";
import Long from "long";
import moment from "moment-timezone";
import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";

import {
  ErrorWithKey,
  getJob,
  JobRequest,
  JobResponseWithKey,
} from "src/api/jobsApi";

import { succeededJobFixture } from "../../jobs/jobsPage/jobsPage.fixture";

import { actions, reducer, JobDetailsReducerState } from "./job.reducer";
import { refreshJobSaga, requestJobSaga } from "./job.sagas";

describe("job sagas", () => {
  const payload = new cockroach.server.serverpb.JobRequest({
    job_id: new Long(8136728577, 70289336),
  });

  const jobID = payload.job_id.toString();

  const jobResponse = new cockroach.server.serverpb.JobResponse(
    succeededJobFixture,
  );

  const jobResponseWithKey: JobResponseWithKey = {
    key: jobID,
    jobResponse: jobResponse,
  };

  const jobAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getJob), jobResponse],
  ];

  const action: PayloadAction<JobRequest> = {
    payload: payload,
    type: "request",
  };

  describe("refreshJobSaga", () => {
    it("dispatches refresh job action", () => {
      return expectSaga(refreshJobSaga, action)
        .provide(jobAPIProvider)
        .put(actions.request(payload))
        .run();
    });
  });

  describe("requestJobSaga", () => {
    const lastUpdated = moment();
    let spy: jest.SpyInstance;

    beforeAll(() => {
      spy = jest.spyOn(moment, "utc").mockImplementation(() => lastUpdated);
    });

    afterAll(() => {
      spy.mockRestore();
    });

    it("successfully requests job", () => {
      return expectSaga(requestJobSaga, action)
        .provide(jobAPIProvider)
        .put(actions.received(jobResponseWithKey))
        .withReducer(reducer)
        .hasFinalState<JobDetailsReducerState>({
          cachedData: {
            [jobID]: {
              data: jobResponseWithKey.jobResponse,
              error: null,
              valid: true,
              inFlight: false,
              lastUpdated,
            },
          },
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      const errorWithKey: ErrorWithKey = {
        key: jobID,
        err: error,
      };
      return expectSaga(requestJobSaga, action)
        .provide([[matchers.call.fn(getJob), throwError(error)]])
        .put(actions.failed(errorWithKey))
        .withReducer(reducer)
        .hasFinalState<JobDetailsReducerState>({
          cachedData: {
            [jobID]: {
              data: null,
              error: error,
              valid: false,
              inFlight: false,
              lastUpdated,
            },
          },
        })
        .run();
    });
  });
});
