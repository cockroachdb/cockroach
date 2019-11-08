// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { Fragment } from "react";
import { Route } from "react-router";
import { Store } from "redux";

import { doLogout, selectLoginState } from "src/redux/login";
import { AdminUIState } from "src/redux/state";
import LoginPage from "src/views/login/loginPage";

export const LOGIN_PAGE = "/login";
export const LOGOUT_PAGE = "/logout";

export default function createLoginRoutes(store: Store<AdminUIState>): JSX.Element {
  function handleLogout(_nextState: any, replace: (route: string) => {}) {
    const loginState = selectLoginState(store.getState());

    if (!loginState.loggedInUser()) {
      return replace(LOGIN_PAGE);
    }

    store.dispatch(doLogout());
  }

  return (
    <Fragment>
      <Route path={LOGIN_PAGE} component={ LoginPage } />
      <Route path={LOGOUT_PAGE} onEnter={ handleLogout } />
    </Fragment>
  );
}
