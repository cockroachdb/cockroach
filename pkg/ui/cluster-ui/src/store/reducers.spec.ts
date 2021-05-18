// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import { createStore } from "redux";
import { rootActions, rootReducer } from "./reducers";
import { actions as statementsActions } from "./statements";

describe("rootReducer", () => {
  it("resets redux state on RESET_STATE action", () => {
    const store = createStore(rootReducer);
    const initState = store.getState();
    const error = new Error("oops!");
    store.dispatch(statementsActions.failed(error));
    const changedState = store.getState();
    store.dispatch(rootActions.resetState());
    const resetState = store.getState();

    assert.deepEqual(initState, resetState);
    assert.notDeepEqual(
      resetState.statements.lastError,
      changedState.statements.lastError,
    );
  });
});
