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
import { ErrorWithKey, getJob, JobResponseWithKey } from "src/api/jobsApi";
import { refreshJobSaga, requestJobSaga } from "./job.sagas";
import { actions, reducer, JobDetailsReducerState } from "./job.reducer";
import { succeededJobFixture } from "../../jobs/jobsPage/jobsPage.fixture";
import Long from "long";

describe("job sagas", () => {
  const payload = new cockroach.server.serverpb.JobRequest({
    job_id: new Long(8136728577, 70289336),
  });
  const jobResponseWithKey: JobResponseWithKey = {
    jobResponse: new cockroach.server.serverpb.JobResponse(succeededJobFixture),
    key: payload.job_id.toString(),
  };

  const jobAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getJob), jobResponseWithKey],
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
        .put(actions.received(jobResponseWithKey))
        .withReducer(reducer)
        .hasFinalState<JobDetailsReducerState>({
          cachedData: {
            [payload.job_id.toString()]: {
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
        key: payload.job_id.toString(),
        err: error,
      };
      return expectSaga(requestJobSaga, actions.request(payload))
        .provide([[matchers.call.fn(getJob), throwError(error)]])
        .put(actions.failed(errorWithKey))
        .withReducer(reducer)
        .hasFinalState<JobDetailsReducerState>({
          cachedData: {
            [payload.job_id.toString()]: {
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
