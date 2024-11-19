// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";

import { DatabasesListResponse, getDatabasesList } from "../../api";

import { actions, DatabasesListState, reducer } from "./databasesList.reducers";
import {
  refreshDatabasesListSaga,
  requestDatabasesListSaga,
} from "./databasesList.saga";

describe("DatabasesList sagas", () => {
  const databasesListResponse: DatabasesListResponse = {
    databases: ["one", "of", "many", "databases"],
    error: {
      message: "sql execution error message!",
      code: "10101",
      severity: "high",
      source: null,
    },
  };
  const databasesListAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getDatabasesList), databasesListResponse],
  ];

  describe("refreshDatabasesListSaga", () => {
    it("dispatches request DatabasesList action", () => {
      return expectSaga(refreshDatabasesListSaga).put(actions.request()).run();
    });
  });

  describe("requestDatabasesListSaga", () => {
    it("request updates inFlight status", () => {
      return expectSaga(refreshDatabasesListSaga)
        .provide(databasesListAPIProvider)
        .put(actions.request())
        .withReducer(reducer)
        .hasFinalState<DatabasesListState>({
          data: null,
          lastError: undefined,
          valid: false,
          inFlight: true,
        })
        .run();
    });

    it("successfully requests databases", () => {
      return expectSaga(requestDatabasesListSaga)
        .provide(databasesListAPIProvider)
        .put(actions.received(databasesListResponse))
        .withReducer(reducer)
        .hasFinalState<DatabasesListState>({
          data: databasesListResponse,
          lastError: null,
          valid: true,
          inFlight: false,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestDatabasesListSaga)
        .provide([[matchers.call.fn(getDatabasesList), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<DatabasesListState>({
          data: null,
          lastError: error,
          valid: false,
          inFlight: false,
        })
        .run();
    });
  });
});
