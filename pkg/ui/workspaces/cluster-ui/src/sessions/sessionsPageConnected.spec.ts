// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import assert from "assert";

import fetchMock from "jest-fetch-mock";
import { applyMiddleware, createStore, Store } from "redux";
import createSagaMiddleware from "redux-saga";

import { rootReducer, sagas } from "src/store";
import {
  actions,
  ICancelQueryRequest,
  ICancelSessionRequest,
} from "src/store/terminateQuery";

class TestDriver {
  private readonly store: Store;

  constructor() {
    const sagaMiddleware = createSagaMiddleware();
    this.store = createStore(rootReducer, {}, applyMiddleware(sagaMiddleware));
    sagaMiddleware.run(sagas);
  }

  async cancelQuery(req: ICancelQueryRequest) {
    return this.store.dispatch(actions.terminateQuery(req));
  }

  async cancelSession(req: ICancelSessionRequest) {
    return this.store.dispatch(actions.terminateSession(req));
  }
}

describe("SessionsPage Connections", () => {
  beforeAll(fetchMock.enableMocks);
  afterEach(fetchMock.resetMocks);
  afterAll(fetchMock.disableMocks);

  describe("cancelQuery", () => {
    it("fires off an HTTP request", async () => {
      const driver = new TestDriver();
      assert.deepStrictEqual(fetchMock.mock.calls.length, 0);
      await driver.cancelQuery({ node_id: "1" });
      assert.deepStrictEqual(
        fetchMock.mock.calls[0][0],
        "_status/cancel_query/1",
      );
    });
  });

  describe("cancelSession", () => {
    it("fires off an HTTP request", async () => {
      const driver = new TestDriver();
      assert.deepStrictEqual(fetchMock.mock.calls.length, 0);
      await driver.cancelSession({ node_id: "1" });
      assert.deepStrictEqual(
        fetchMock.mock.calls[0][0],
        "_status/cancel_session/1",
      );
    });
  });
});
