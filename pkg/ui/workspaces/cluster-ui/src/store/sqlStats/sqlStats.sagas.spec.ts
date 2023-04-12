// Copyright 2021 The Cockroach Authors.
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

import { getCombinedStatements } from "src/api/statementsApi";
import { resetSQLStats } from "src/api/sqlStatsApi";
import {
  refreshSQLStatsSaga,
  requestSQLStatsSaga,
  resetSQLStatsSaga,
} from "./sqlStats.sagas";
import { actions, reducer, SQLStatsState } from "./sqlStats.reducer";
import { actions as sqlDetailsStatsActions } from "../statementDetails/statementDetails.reducer";
import Long from "long";
import moment from "moment-timezone";

const lastUpdated = moment();

describe("SQLStats sagas", () => {
  let spy: jest.SpyInstance;
  beforeAll(() => {
    spy = jest.spyOn(moment, "utc").mockImplementation(() => lastUpdated);
  });

  afterAll(() => {
    spy.mockRestore();
  });

  const payload = new cockroach.server.serverpb.CombinedStatementsStatsRequest({
    start: Long.fromNumber(1596816675),
    end: Long.fromNumber(1596820675),
  });
  const sqlStatsResponse = new cockroach.server.serverpb.StatementsResponse({
    statements: [
      {
        id: new Long(1),
      },
      {
        id: new Long(2),
      },
    ],
    last_reset: null,
  });

  const stmtStatsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getCombinedStatements), sqlStatsResponse],
  ];

  describe("refreshSQLStatsSaga", () => {
    it("dispatches request SQLStats action", () => {
      return expectSaga(refreshSQLStatsSaga, actions.request(payload))
        .provide(stmtStatsAPIProvider)
        .put(actions.request(payload))
        .run();
    });
  });

  describe("requestSQLStatsSaga", () => {
    it("successfully requests statements list", () => {
      return expectSaga(requestSQLStatsSaga, actions.request(payload))
        .provide(stmtStatsAPIProvider)
        .put(actions.received(sqlStatsResponse))
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: sqlStatsResponse,
          error: null,
          valid: true,
          lastUpdated,
          inFlight: false,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestSQLStatsSaga, actions.request(payload))
        .provide([[matchers.call.fn(getCombinedStatements), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: null,
          error: error,
          valid: false,
          lastUpdated,
          inFlight: false,
        })
        .run();
    });
  });

  describe("resetSQLStatsSaga", () => {
    const resetSQLStatsResponse =
      new cockroach.server.serverpb.ResetSQLStatsResponse();

    it("successfully resets SQL stats", () => {
      return expectSaga(resetSQLStatsSaga)
        .provide([[matchers.call.fn(resetSQLStats), resetSQLStatsResponse]])
        .put(sqlDetailsStatsActions.invalidateAll())
        .put(actions.invalidated())
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: null,
          error: null,
          valid: false,
          lastUpdated: null,
          inFlight: false,
        })
        .run();
    });

    it("returns error on failed reset", () => {
      const err = new Error("failed to reset");
      return expectSaga(resetSQLStatsSaga)
        .provide([[matchers.call.fn(resetSQLStats), throwError(err)]])
        .put(actions.failed(err))
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: null,
          error: err,
          valid: false,
          lastUpdated,
          inFlight: false,
        })
        .run();
    });
  });
});
