import { Location } from "history";
import { Action } from "redux";
import { ThunkAction } from "redux-thunk";
import { createSelector } from "reselect";

import { createPath } from "src/hacks/createPath";
import { userLogin } from "src/util/api";
import { AdminUIState } from "src/redux/state";
import { LOGIN_PAGE } from "src/routes/login";
import { cockroach } from "src/js/protos";
import { getDataFromServer } from "src/util/dataFromServer";

import UserLoginRequest = cockroach.server.serverpb.UserLoginRequest;

const dataFromServer = getDataFromServer();

// State for application use.

export interface LoginState {
    useLogin(): boolean;
    loginEnabled(): boolean;
    hasAccess(): boolean;
    loggedInUser(): string;
}

class LoginEnabledState {
    apiState: LoginAPIState;

    constructor(state: LoginAPIState) {
        this.apiState = state;
    }

    useLogin(): boolean {
        return true;
    }

    loginEnabled(): boolean {
        return true;
    }

    hasAccess(): boolean {
        return this.apiState.loggedInUser != null;
    }

    loggedInUser(): string {
        return this.apiState.loggedInUser;
    }
}

class LoginDisabledState {
    useLogin(): boolean {
        return true;
    }

    loginEnabled(): boolean {
        return false;
    }

    hasAccess(): boolean {
        return true;
    }

    loggedInUser(): string {
        return null;
    }
}

class NoLoginState {
    useLogin(): boolean {
        return false;
    }

    loginEnabled(): boolean {
        return false;
    }

    hasAccess(): boolean {
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

export function getLoginPage(location: Location) {
  const query = !location ? undefined : {
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
  error: string;
  inProgress: boolean;
}

const emptyLoginState: LoginAPIState = {
  loggedInUser: dataFromServer.LoggedInUser,
  error: null,
  inProgress: false,
};

// Actions

const LOGIN_BEGIN = "LOGIN_BEGIN";
const LOGIN_SUCCESS = "LOGIN_SUCCESS";
const LOGIN_FAILURE = "LOGIN_FAILURE";

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
  error: string;
}

function loginFailure(error: string): LoginFailureAction {
  return {
    type: LOGIN_FAILURE,
    error,
  };
}

export function doLogin(username: string, password: string): ThunkAction<Promise<void>, AdminUIState, void> {
  return (dispatch) => {
    dispatch(loginBeginAction);

    const loginReq = new UserLoginRequest({
      username,
      password,
    });
    return userLogin(loginReq)
      .then(
        () => { dispatch(loginSuccess(username)); },
        (err) => { dispatch(loginFailure(err.toString())); },
      );
  };
}

// Reducer

export function loginReducer(state = emptyLoginState, action: Action): LoginAPIState {
  switch (action.type) {
    case LOGIN_BEGIN:
      return {
        loggedInUser: null,
        error: null,
        inProgress: true,
      };
    case LOGIN_SUCCESS:
      return {
        loggedInUser: (action as LoginSuccessAction).loggedInUser,
        inProgress: false,
        error: null,
      };
    case LOGIN_FAILURE:
      return {
        loggedInUser: null,
        inProgress: false,
        error: (action as LoginFailureAction).error,
      };
    default:
      return state;
  }
}
