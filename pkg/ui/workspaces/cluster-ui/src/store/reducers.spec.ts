// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createStore } from "redux";

import { actions as localStorageActions } from "./localStorage";
import { rootReducer } from "./reducers";
import { rootActions } from "./rootActions";

describe("rootReducer", () => {
  it("resets redux state on RESET_STATE action", () => {
    const store = createStore(rootReducer);
    const initState = store.getState();
    store.dispatch(
      localStorageActions.updateTimeScale({
        value: {
          windowSize: null,
          sampleSize: null,
          fixedWindowEnd: null,
          key: "Past 1 Hour",
        },
      }),
    );
    const changedState = store.getState();
    store.dispatch(rootActions.resetState());
    const resetState = store.getState();

    expect(initState).toEqual(resetState);
    expect(resetState.localStorage).not.toEqual(changedState.localStorage);
  });
});
