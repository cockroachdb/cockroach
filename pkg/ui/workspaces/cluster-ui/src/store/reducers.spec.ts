// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";
import { createStore } from "redux";

import { rootReducer } from "./reducers";
import { rootActions } from "./rootActions";
import { actions as sqlStatsActions } from "./sqlStats";

describe("rootReducer", () => {
  it("resets redux state on RESET_STATE action", () => {
    const store = createStore(rootReducer);
    const initState = store.getState();
    const error = new Error("oops!");
    store.dispatch(sqlStatsActions.failed(error));
    const changedState = store.getState();
    store.dispatch(rootActions.resetState());
    const resetState = store.getState();

    assert.deepEqual(initState, resetState);
    assert.notDeepEqual(
      resetState.statements.error,
      changedState.statements.error,
    );
  });
});
