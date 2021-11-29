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

import { getStatements, getCombinedStatements } from "src/api/statementsApi";
import { resetSQLStats } from "src/api/sqlStatsApi";
import {
  receivedSQLStatsSaga,
  refreshSQLStatsSaga,
  requestSQLStatsSaga,
  resetSQLStatsSaga,
} from "./sqlStats.sagas";
import { actions, reducer, SQLStatsState } from "./sqlStats.reducer";
import Long from "long";

describe("SQLStats sagas", () => {
  const combinedSQLStatsResponse = new cockroach.server.serverpb.StatementsResponse(
    {
      statements: [
        {
          id: new Long(1),
        },
        {
          id: new Long(2),
        },
      ],
      last_reset: null,
    },
  );
  const nonCombinedSQLStatsResponse = new cockroach.server.serverpb.StatementsResponse(
    {
      statements: [
        {
          id: new Long(1),
        },
      ],
      last_reset: null,
    },
  );

  const stmtStatsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getCombinedStatements), combinedSQLStatsResponse],
    [matchers.call.fn(getStatements), nonCombinedSQLStatsResponse],
  ];

  describe("refreshSQLStatsSaga", () => {
    it("dispatches request SQLStats action", () => {
      return expectSaga(refreshSQLStatsSaga)
        .put(actions.request())
        .run();
    });
  });

  describe("requestSQLStatsSaga", () => {
    it("successfully requests statements list", () => {
      return expectSaga(requestSQLStatsSaga)
        .provide(stmtStatsAPIProvider)
        .put(actions.received(nonCombinedSQLStatsResponse))
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: nonCombinedSQLStatsResponse,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("requests combined SQL Stats if combined=true in the request message", () => {
      return expectSaga(requestSQLStatsSaga, {
        payload: new cockroach.server.serverpb.StatementsRequest({
          combined: true,
        }),
      })
        .provide(stmtStatsAPIProvider)
        .put(actions.received(combinedSQLStatsResponse))
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: combinedSQLStatsResponse,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("requests combined SQL Stats if combined=false in the request message", () => {
      return expectSaga(requestSQLStatsSaga, {
        payload: new cockroach.server.serverpb.StatementsRequest({
          combined: false,
        }),
      })
        .provide(stmtStatsAPIProvider)
        .put(actions.received(nonCombinedSQLStatsResponse))
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: nonCombinedSQLStatsResponse,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestSQLStatsSaga)
        .provide([[matchers.call.fn(getStatements), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: null,
          lastError: error,
          valid: false,
        })
        .run();
    });
  });

  describe("receivedSQLStatsSaga", () => {
    it("sets valid status to false after specified period of time", () => {
      const timeout = 500;
      return expectSaga(receivedSQLStatsSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: combinedSQLStatsResponse,
          lastError: null,
          valid: true,
        })
        .hasFinalState<SQLStatsState>({
          data: combinedSQLStatsResponse,
          lastError: null,
          valid: false,
        })
        .run(1000);
    });
  });

  describe("resetSQLStatsSaga", () => {
    const resetSQLStatsResponse = new cockroach.server.serverpb.ResetSQLStatsResponse();

    it("successfully resets SQL stats", () => {
      return expectSaga(resetSQLStatsSaga)
        .provide([[matchers.call.fn(resetSQLStats), resetSQLStatsResponse]])
        .put(actions.invalidated())
        .put(actions.refresh())
        .withReducer(reducer)
        .hasFinalState<SQLStatsState>({
          data: null,
          lastError: null,
          valid: false,
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
          lastError: err,
          valid: false,
        })
        .run();
    });
  });
});
