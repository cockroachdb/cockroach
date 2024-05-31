// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "@reduxjs/toolkit";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";
import * as matchers from "redux-saga-test-plan/matchers";
import { expectSaga } from "redux-saga-test-plan";

import {
  DatabaseDetailsSpanStatsReqParams,
  DatabaseDetailsSpanStatsResponse,
  getDatabaseDetailsSpanStats,
  SqlApiResponse,
} from "../../api";

import {
  databaseDetailsSpanStatsReducer,
  KeyedDatabaseDetailsSpanStatsState,
} from "./databaseDetails.reducer";
import {
  refreshDatabaseDetailsSpanStatsSaga,
  requestDatabaseDetailsSpanStatsSaga,
} from "./databaseDetailsSpanStats.saga";
const { actions, reducer } = databaseDetailsSpanStatsReducer;

describe("DatabaseDetails sagas", () => {
  const database = "test_db";
  const requestAction: PayloadAction<DatabaseDetailsSpanStatsReqParams> = {
    payload: { database },
    type: "request",
  };
  const spanStatsResponse: SqlApiResponse<DatabaseDetailsSpanStatsResponse> = {
    maxSizeReached: false,
    results: {
      spanStats: {
        approximate_disk_bytes: 100,
        live_bytes: 200,
        range_count: 300,
        total_bytes: 400,
        error: undefined,
      },
    },
  };
  const provider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getDatabaseDetailsSpanStats), spanStatsResponse],
  ];

  describe("refreshSpanStatsSaga", () => {
    it("dispatches request span stats action", () => {
      return expectSaga(refreshDatabaseDetailsSpanStatsSaga, requestAction)
        .put(actions.request(requestAction.payload))
        .run();
    });
  });

  describe("request span stats saga", () => {
    it("successfully requests span stats", () => {
      return expectSaga(requestDatabaseDetailsSpanStatsSaga, requestAction)
        .provide(provider)
        .put(
          actions.received({
            response: spanStatsResponse,
            key: database,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<KeyedDatabaseDetailsSpanStatsState>({
          [database]: {
            data: spanStatsResponse,
            lastError: null,
            valid: true,
            inFlight: false,
          },
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestDatabaseDetailsSpanStatsSaga, requestAction)
        .provide([
          [matchers.call.fn(getDatabaseDetailsSpanStats), throwError(error)],
        ])
        .put(
          actions.failed({
            err: error,
            key: database,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<KeyedDatabaseDetailsSpanStatsState>({
          [database]: {
            data: null,
            lastError: error,
            valid: false,
            inFlight: false,
          },
        })
        .run();
    });
  });
});
