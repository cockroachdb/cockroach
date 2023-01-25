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
import {
  ErrorWithKey,
  getJob,
  JobRequest,
  JobResponseWithKey,
} from "src/api/jobsApi";
import { refreshJobSaga, requestJobSaga } from "./job.sagas";
import { actions, reducer, JobDetailsReducerState } from "./job.reducer";
import { succeededJobFixture } from "../../jobs/jobsPage/jobsPage.fixture";
import Long from "long";
import { PayloadAction } from "@reduxjs/toolkit";

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
    it("successfully requests job", () => {
      return expectSaga(requestJobSaga, action)
        .provide(jobAPIProvider)
        .put(actions.received(jobResponseWithKey))
        .withReducer(reducer)
        .hasFinalState<JobDetailsReducerState>({
          cachedData: {
            [jobID]: {
              data: jobResponseWithKey.jobResponse,
              lastError: null,
              valid: true,
              inFlight: false,
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
              lastError: error,
              valid: false,
              inFlight: false,
            },
          },
        })
        .run();
    });
  });
});
