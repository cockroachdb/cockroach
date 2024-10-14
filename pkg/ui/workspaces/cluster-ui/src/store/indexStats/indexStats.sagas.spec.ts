// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { PayloadAction } from "@reduxjs/toolkit";
import Long from "long";
import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";

import {
  getIndexStats,
  resetIndexStats,
  TableIndexStatsRequest,
} from "../../api/indexDetailsApi";
import { generateTableID } from "../../util";

import {
  actions,
  IndexStatsReducerState,
  reducer,
  ResetIndexUsageStatsPayload,
} from "./indexStats.reducer";
import {
  refreshIndexStatsSaga,
  requestIndexStatsSaga,
  resetIndexStatsSaga,
} from "./indexStats.sagas";

import RecommendationType = cockroach.sql.IndexRecommendation.RecommendationType;

describe("IndexStats sagas", () => {
  const database = "test_db";
  const table = "test_table";
  const requestAction: PayloadAction<TableIndexStatsRequest> = {
    payload: new cockroach.server.serverpb.TableIndexStatsRequest({
      database: database,
      table: table,
    }),
    type: "request",
  };
  const resetAction: PayloadAction<ResetIndexUsageStatsPayload> = {
    payload: {
      database: database,
      table: table,
    },
    type: "reset",
  };
  const key = generateTableID(database, table);
  const tableIndexStatsResponse =
    new cockroach.server.serverpb.TableIndexStatsResponse({
      statistics: [
        {
          statistics: {
            key: {
              table_id: 1,
              index_id: 2,
            },
            stats: {
              total_read_count: Long.fromInt(0, true),
              last_read: null,
              total_rows_read: Long.fromInt(0, true),
              total_write_count: Long.fromInt(1, true),
              last_write: null,
              total_rows_written: Long.fromInt(5, true),
            },
          },
          index_name: "test_index",
          index_type: "secondary",
          create_statement: "mock create statement",
          created_at: null,
        },
      ],
      database_id: 10,
      last_reset: null,
      index_recommendations: [
        {
          table_id: 1,
          index_id: 2,
          type: RecommendationType.DROP_UNUSED,
          reason: "mock reason",
        },
      ],
    });
  const resetIndexStatsResponse =
    new cockroach.server.serverpb.ResetIndexUsageStatsResponse();
  const indexStatsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getIndexStats), tableIndexStatsResponse],
    [matchers.call.fn(resetIndexStats), resetIndexStatsResponse],
  ];

  describe("refreshIndexStatsSaga", () => {
    it("dispatches request IndexStats action", () => {
      return expectSaga(refreshIndexStatsSaga).put(actions.request()).run();
    });
  });

  describe("requestIndexStatsSaga", () => {
    it("successfully requests index stats", () => {
      return expectSaga(requestIndexStatsSaga, requestAction)
        .provide(indexStatsAPIProvider)
        .put(
          actions.received({
            indexStatsResponse: tableIndexStatsResponse,
            key,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<IndexStatsReducerState>({
          "test_db/test_table": {
            data: tableIndexStatsResponse,
            lastError: null,
            valid: true,
            inFlight: false,
          },
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestIndexStatsSaga, requestAction)
        .provide([[matchers.call.fn(getIndexStats), throwError(error)]])
        .put(
          actions.failed({
            err: error,
            key,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<IndexStatsReducerState>({
          "test_db/test_table": {
            data: null,
            lastError: error,
            valid: false,
            inFlight: false,
          },
        })
        .run();
    });
  });

  describe("resetIndexStatsSaga", () => {
    it("successfully resets index stats", () => {
      return expectSaga(resetIndexStatsSaga, resetAction)
        .provide(indexStatsAPIProvider)
        .put(actions.invalidateAll())
        .put(
          actions.refresh(
            new cockroach.server.serverpb.TableIndexStatsRequest({
              ...resetAction.payload,
            }),
          ),
        )
        .withReducer(reducer)
        .hasFinalState<IndexStatsReducerState>({
          "test_db/test_table": {
            data: null,
            valid: false,
            lastError: null,
            inFlight: true,
          },
        })
        .run();
    });

    it("returns error on failed reset", () => {
      const err = new Error("failed to reset");
      return expectSaga(resetIndexStatsSaga, resetAction)
        .provide([[matchers.call.fn(resetIndexStats), throwError(err)]])
        .put(
          actions.failed({
            err: err,
            key,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<IndexStatsReducerState>({
          "test_db/test_table": {
            data: null,
            lastError: err,
            valid: false,
            inFlight: false,
          },
        })
        .run();
    });
  });
});
