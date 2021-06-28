// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { emptyLoginState } from "src/redux/login";
import { LoginPageProps } from "./loginPage";
import { createMemoryHistory } from "history";
import { RouteComponentProps } from "react-router-dom";

const history = createMemoryHistory({ initialEntries: ["/statements"] });

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
