// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { storiesOf } from "@storybook/react";
import { ConnectedRouter, connectRouter } from "connected-react-router";
import { createMemoryHistory } from "history";
import React from "react";
import { Provider } from "react-redux";
import { combineReducers, createStore } from "redux";
import { RenderFunction } from "storybook__react";
import { LoginPage } from "./loginPage";
import { loginPagePropsFixture, loginPagePropsLoadingFixture, loginPagePropsErrorFixture } from "./loginPage.fixture";

const history = createMemoryHistory();
const routerReducer = connectRouter(history);

const store = createStore(combineReducers({
  router: routerReducer,
}));

const withRouterProvider = (storyFn: RenderFunction) => (
  <Provider store={store}>
    <ConnectedRouter history={history}>
      { storyFn() }
    </ConnectedRouter>
  </Provider>
);

storiesOf("LoginPage", module)
  .addDecorator(withRouterProvider)
  .add("Default state", () => (
    <LoginPage {...loginPagePropsFixture} />
  ))
  .add("Loading state", () => (
    <LoginPage {...loginPagePropsLoadingFixture} />
  ))
  .add("Error state", () => (
    <LoginPage {...loginPagePropsErrorFixture} />
  ));
