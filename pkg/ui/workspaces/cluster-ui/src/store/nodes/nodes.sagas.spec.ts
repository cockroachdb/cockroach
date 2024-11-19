// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import { throwError } from "redux-saga-test-plan/providers";

import { getNodes } from "src/api/nodesApi";

import { accumulateMetrics } from "../../util";

import { getNodesResponse } from "./nodes.fixtures";
import { actions, reducer, NodesState } from "./nodes.reducer";
import {
  receivedNodesSaga,
  requestNodesSaga,
  refreshNodesSaga,
} from "./nodes.sagas";

describe("Nodes sagas", () => {
  const nodesResponse = getNodesResponse();
  const nodes = accumulateMetrics(nodesResponse.nodes);

  describe("refreshNodesSaga", () => {
    it("dispatches request nodes action", () => {
      expectSaga(refreshNodesSaga).put(actions.request()).run();
    });
  });

  describe("requestNodesSaga", () => {
    it("successfully requests nodes list", () => {
      expectSaga(requestNodesSaga)
        .provide([[matchers.call.fn(getNodes), nodesResponse]])
        .put(actions.received(nodes))
        .withReducer(reducer)
        .hasFinalState<NodesState>({
          data: nodes,
          lastError: null,
          valid: true,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      expectSaga(requestNodesSaga)
        .provide([[matchers.call.fn(getNodes), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<NodesState>({
          data: null,
          lastError: error,
          valid: false,
        })
        .run();
    });
  });

  describe("receivedNodesSaga", () => {
    it("sets valid status to false after specified period of time", () => {
      const timeout = 500;
      expectSaga(receivedNodesSaga, timeout)
        .delay(timeout)
        .put(actions.invalidated())
        .withReducer(reducer, {
          data: nodes,
          lastError: null,
          valid: true,
        })
        .hasFinalState<NodesState>({
          data: nodes,
          lastError: null,
          valid: false,
        })
        .run(1000);
    });
  });
});
