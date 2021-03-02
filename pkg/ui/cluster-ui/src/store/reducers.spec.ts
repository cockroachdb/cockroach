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
