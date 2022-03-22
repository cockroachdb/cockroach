// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { mount, ReactWrapper } from "enzyme";
import { Action, Store } from "redux";
import { Provider } from "react-redux";
import { ConnectedRouter } from "connected-react-router";
import { createMemoryHistory } from "history";

import "src/enzymeInit";
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
