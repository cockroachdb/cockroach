// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Location, createPath } from "history";
import { Action } from "redux";
import { createSelector } from "reselect";

import { AdminUIState } from "src/redux/state";
import { LOGIN_PAGE, LOGOUT_PAGE } from "src/routes/login";
import { userLogout } from "src/util/api";
import { getDataFromServer } from "src/util/dataFromServer";

import { clearTenantCookie } from "./cookies";

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

class InsecureState {
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
    return "";
  }
}

// Selector

export const selectLoginState = createSelector(
  (state: AdminUIState) => state.login,
  (login: LoginAPIState) => {
    const dataFromServer = getDataFromServer();
    if (dataFromServer.Insecure) {
      return new InsecureState();
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
  const redirectTo = !shouldRedirect(location)
    ? undefined
    : createPath({
        pathname: location.pathname,
        search: location.search,
      });
  return {
    pathname: LOGIN_PAGE,
    search: `?redirectTo=${encodeURIComponent(redirectTo)}`,
  };
}

// Redux implementation.

// LoginAPIState is the Redux state for login. Only loggedInUser is
// managed via Redux — it's the one piece of login state that other
// components (requireLogin, loginIndicator, alertDataProvider) need.
export interface LoginAPIState {
  loggedInUser: string;
}

export const emptyLoginState: LoginAPIState = {
  loggedInUser: dataFromServer.LoggedInUser,
};

// Actions

const LOGIN_SUCCESS = "cockroachui/auth/LOGIN_SUCCESS";

interface LoginSuccessAction extends Action {
  type: typeof LOGIN_SUCCESS;
  loggedInUser: string;
}

export function loginSuccess(loggedInUser: string): LoginSuccessAction {
  return {
    type: LOGIN_SUCCESS,
    loggedInUser,
  };
}

export function doLogout(): void {
  // Clearing the tenant cookie on logout is necessary in order to
  // avoid routing login requests to that specific tenant.
  clearTenantCookie();
  // Make request to log out, reloading the page whether it succeeds or not.
  // If there was a successful log out but the network dropped the response somehow,
  // you'll get the login page on reload. If The logout actually didn't work, you'll
  // be reloaded to the same page and can try to log out again.
  userLogout().then(
    () => {
      document.location.reload();
    },
    () => {
      document.location.reload();
    },
  );
}

// Reducer

export function loginReducer(
  state = emptyLoginState,
  action: Action,
): LoginAPIState {
  switch (action.type) {
    case LOGIN_SUCCESS:
      return {
        ...state,
        loggedInUser: (action as LoginSuccessAction).loggedInUser,
      };
    default:
      return state;
  }
}
