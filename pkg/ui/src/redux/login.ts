// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Location, createPath } from "history";
import { Action } from "redux";
import { ThunkAction } from "redux-thunk";
import { createSelector } from "reselect";

import { userLogin, userLogout } from "src/util/api";
import { AdminUIState } from "src/redux/state";
import { LOGIN_PAGE, LOGOUT_PAGE } from "src/routes/login";
import { cockroach } from "src/js/protos";
import { getDataFromServer } from "src/util/dataFromServer";

import UserLoginRequest = cockroach.server.serverpb.UserLoginRequest;

const dataFromServer = getDataFromServer();

// State for application use.

export interface LoginState {
  // displayUserMenu() indicates whether the login drop-down menu should be
  // displayed at the top right.
  displayUserMenu(): boolean;
  // secureCluster() indicates whether the connection is secure. If
  // false, an "insecure" indicator is displayed at the top right.
  secureCluster(): boolean;
  // hideLoginPage() indicates whether the login page can be
  // displayed at all. The login page is hidden e.g.
  // after a user has logged in.
  hideLoginPage(): boolean;
  // loggedInUser() returns the name of the user logged in.
  loggedInUser(): string;
}

class LoginEnabledState {
  apiState: LoginAPIState;

  constructor(state: LoginAPIState) {
    this.apiState = state;
  }

  displayUserMenu(): boolean {
    return true;
  }

  secureCluster(): boolean {
    return true;
  }

  hideLoginPage(): boolean {
    return this.apiState.loggedInUser != null;
  }

  loggedInUser(): string {
    return this.apiState.loggedInUser;
  }
}

class LoginDisabledState {
  displayUserMenu(): boolean {
    return true;
  }

  secureCluster(): boolean {
    return false;
  }

  hideLoginPage(): boolean {
    return true;
  }

  loggedInUser(): string {
    return null;
  }
}

class NoLoginState {
  displayUserMenu(): boolean {
    return false;
  }

  secureCluster(): boolean {
    return false;
  }

  hideLoginPage(): boolean {
    return true;
  }

  loggedInUser(): string {
    return null;
  }
}

// Selector

export const selectLoginState = createSelector(
  (state: AdminUIState) => state.login,
  (login: LoginAPIState) => {
    if (!dataFromServer.ExperimentalUseLogin) {
      return new NoLoginState();
    }

    if (!dataFromServer.LoginEnabled) {
      return new LoginDisabledState();
    }

    return new LoginEnabledState(login);
  },
);

function shouldRedirect(location: Location) {
  if (!location) {
    return false;
  }

  if (location.pathname === LOGOUT_PAGE) {
    return false;
  }

  return true;
}

export function getLoginPage(location: Location) {
  const query = !shouldRedirect(location)
    ? undefined
    : {
        redirectTo: createPath({
          pathname: location.pathname,
          search: location.search,
        }),
      };
  return {
    pathname: LOGIN_PAGE,
    query: query,
  };
}

// Redux implementation.

// State

export interface LoginAPIState {
  loggedInUser: string;
  error: Error;
  inProgress: boolean;
  oidcAutoLogin: boolean;
  oidcLoginEnabled: boolean;
  oidcButtonText: string;
}

export const emptyLoginState: LoginAPIState = {
  loggedInUser: dataFromServer.LoggedInUser,
  error: null,
  inProgress: false,
  oidcAutoLogin: dataFromServer.OIDCAutoLogin,
  oidcLoginEnabled: dataFromServer.OIDCLoginEnabled,
  oidcButtonText: dataFromServer.OIDCButtonText,
};

// Actions

const LOGIN_BEGIN = "cockroachui/auth/LOGIN_BEGIN";
const LOGIN_SUCCESS = "cockroachui/auth/LOGIN_SUCCESS";
const LOGIN_FAILURE = "cockroachui/auth/LOGIN_FAILURE";

const loginBeginAction = {
  type: LOGIN_BEGIN,
};

interface LoginSuccessAction extends Action {
  type: typeof LOGIN_SUCCESS;
  loggedInUser: string;
}

function loginSuccess(loggedInUser: string): LoginSuccessAction {
  return {
    type: LOGIN_SUCCESS,
    loggedInUser,
  };
}

interface LoginFailureAction extends Action {
  type: typeof LOGIN_FAILURE;
  error: Error;
}

function loginFailure(error: Error): LoginFailureAction {
  return {
    type: LOGIN_FAILURE,
    error,
  };
}

const LOGOUT_BEGIN = "cockroachui/auth/LOGOUT_BEGIN";

const logoutBeginAction = {
  type: LOGOUT_BEGIN,
};

export function doLogin(
  username: string,
  password: string,
): ThunkAction<Promise<void>, AdminUIState, void> {
  return (dispatch) => {
    dispatch(loginBeginAction);

    const loginReq = new UserLoginRequest({
      username,
      password,
    });
    return userLogin(loginReq).then(
      () => {
        dispatch(loginSuccess(username));
      },
      (err) => {
        dispatch(loginFailure(err));
      },
    );
  };
}

export function doLogout(): ThunkAction<Promise<void>, AdminUIState, void> {
  return (dispatch) => {
    dispatch(logoutBeginAction);

    // Make request to log out, reloading the page whether it succeeds or not.
    // If there was a successful log out but the network dropped the response somehow,
    // you'll get the login page on reload. If The logout actually didn't work, you'll
    // be reloaded to the same page and can try to log out again.
    return userLogout().then(
      () => {
        document.location.reload();
      },
      () => {
        document.location.reload();
      },
    );
  };
}

// Reducer

export function loginReducer(
  state = emptyLoginState,
  action: Action,
): LoginAPIState {
  switch (action.type) {
    case LOGIN_BEGIN:
      return {
        ...state,
        loggedInUser: null,
        error: null,
        inProgress: true,
      };
    case LOGIN_SUCCESS:
      return {
        ...state,
        loggedInUser: (action as LoginSuccessAction).loggedInUser,
        inProgress: false,
        error: null,
      };
    case LOGIN_FAILURE:
      return {
        ...state,
        loggedInUser: null,
        inProgress: false,
        error: (action as LoginFailureAction).error,
      };
    case LOGOUT_BEGIN:
      return {
        ...state,
        loggedInUser: state.loggedInUser,
        inProgress: true,
        error: null,
      };
    default:
      return state;
  }
}
