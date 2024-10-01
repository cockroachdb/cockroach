// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ConnectedRouter } from "connected-react-router";
import { mount, ReactWrapper } from "enzyme";
import { createMemoryHistory } from "history";
import React from "react";
import { Provider } from "react-redux";
import { Action, Store } from "redux";

import { AdminUIState, createAdminUIStore } from "src/redux/state";

export function connectedMount(
  nodeFactory: (store: Store<AdminUIState>) => React.ReactNode,
) {
  const history = createMemoryHistory({
    initialEntries: ["/"],
  });
  const store: Store<AdminUIState, Action> = createAdminUIStore(history);
  const wrapper: ReactWrapper = mount(
    <Provider store={store}>
      <ConnectedRouter history={history}>{nodeFactory(store)}</ConnectedRouter>
    </Provider>,
  );
  return wrapper;
}
