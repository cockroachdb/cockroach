// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Route, Redirect } from "react-router-dom";
import { Store } from "redux";

import { doLogout, selectLoginState } from "src/redux/login";
import { AdminUIState, AppDispatch } from "src/redux/state";
import LoginPage from "src/views/login/loginPage";

export const LOGIN_PAGE = "/login";
export const LOGOUT_PAGE = "/logout";

export function createLoginRoute() {
  return <Route exact path={LOGIN_PAGE} component={LoginPage} />;
}

export function createLogoutRoute(store: Store<AdminUIState>): JSX.Element {
  return (
    <Route
      exact
      path={LOGOUT_PAGE}
      render={() => {
        const loginState = selectLoginState(store.getState());

        if (!loginState.loggedInUser()) {
          return <Redirect to={LOGIN_PAGE} />;
        }

        (store.dispatch as AppDispatch)(doLogout());
      }}
    />
  );
}
