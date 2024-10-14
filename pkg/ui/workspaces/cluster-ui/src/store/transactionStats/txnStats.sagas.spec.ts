// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import moment from "moment-timezone";
import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";

import { getFlushedTxnStatsApi } from "src/api/statementsApi";

import { actions, reducer, TxnStatsState } from "./txnStats.reducer";
import { refreshTxnStatsSaga, requestTxnStatsSaga } from "./txnStats.sagas";

const lastUpdated = moment();

describe("txnStats sagas", () => {
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
    limit: Long.fromNumber(100),
  });

  const txnStatsResponse = new cockroach.server.serverpb.StatementsResponse({
    transactions: [
      {
        stats_data: { transaction_fingerprint_id: new Long(1) },
      },
      {
        stats_data: { transaction_fingerprint_id: new Long(2) },
      },
    ],
    last_reset: null,
  });

  const txnStatsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getFlushedTxnStatsApi), txnStatsResponse],
  ];

  describe("refreshTxnStatsSaga", () => {
    it("dispatches request txnStats action", () => {
      return expectSaga(refreshTxnStatsSaga, actions.request(payload))
        .provide(txnStatsAPIProvider)
        .put(actions.request(payload))
        .run();
    });
  });

  describe("requestTxnStatsSaga", () => {
    it("successfully requests statements list", () => {
      return expectSaga(requestTxnStatsSaga, actions.request(payload))
        .provide(txnStatsAPIProvider)
        .put(actions.received(txnStatsResponse))
        .withReducer(reducer)
        .hasFinalState<TxnStatsState>({
          inFlight: false,
          data: txnStatsResponse,
          error: null,
          valid: true,
          lastUpdated,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestTxnStatsSaga, actions.request(payload))
        .provide([[matchers.call.fn(getFlushedTxnStatsApi), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<TxnStatsState>({
          inFlight: false,
          data: null,
          error: error,
          valid: false,
          lastUpdated,
        })
        .run();
    });
  });
});
