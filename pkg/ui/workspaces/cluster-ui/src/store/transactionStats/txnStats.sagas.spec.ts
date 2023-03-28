// Copyright 2023 The Cockroach Authors.
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

import { getFlushedTxnStatsApi } from "src/api/statementsApi";
import { refreshTxnStatsSaga, requestTxnStatsSaga } from "./txnStats.sagas";
import { actions, reducer, TxnStatsState } from "./txnStats.reducer";
import Long from "long";
import moment from "moment-timezone";

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
          lastError: null,
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
          lastError: error,
          valid: false,
          lastUpdated,
        })
        .run();
    });
  });
});
