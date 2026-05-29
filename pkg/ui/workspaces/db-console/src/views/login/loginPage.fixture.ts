// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createMemoryHistory } from "history";
import { RouteComponentProps } from "react-router-dom";

import type { LoginPageProps, LoginPageState } from "./loginPage";

const history = createMemoryHistory({ initialEntries: ["/statements"] });

const emptyLoginState: LoginPageState = {
  loggedInUser: "",
  error: null,
  inProgress: false,
  oidcAutoLogin: false,
  oidcLoginEnabled: false,
  oidcButtonText: "",
  oidcGenerateJWTAuthTokenEnabled: false,
};

export const loginPagePropsFixture: LoginPageProps & RouteComponentProps = {
  history,
  location: {
    pathname: "/login",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/login",
    url: "/login",
    isExact: true,
    params: {},
  },
  loginState: emptyLoginState,
  handleLogin: (() => {}) as any,
};

export const loginPagePropsLoadingFixture = {
  ...loginPagePropsFixture,
  loginState: {
    ...emptyLoginState,
    inProgress: true,
  },
};

export const loginPagePropsErrorFixture = {
  ...loginPagePropsFixture,
  loginState: {
    ...emptyLoginState,
    error: {
      name: "error",
      message: "Invalid username or password.",
    },
  },
};
