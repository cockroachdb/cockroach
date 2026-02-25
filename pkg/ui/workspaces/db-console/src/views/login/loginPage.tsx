// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useEffect, useState } from "react";
import Helmet from "react-helmet";
import { useDispatch, useSelector } from "react-redux";
import { RouteComponentProps } from "react-router-dom";

import ErrorCircle from "assets/error-circle.svg";
import {
  CockroachLabsLockupIcon,
  Button,
  TextInput,
  PasswordInput,
  Text,
  TextTypes,
} from "src/components";
import { doLogin, LoginAPIState } from "src/redux/login";
import { AdminUIState, AppDispatch } from "src/redux/state";
import * as docsURL from "src/util/docs";
import {
  OIDCGenerateJWTAuthTokenConnected,
  OIDCLoginConnected,
} from "src/views/login/oidc";

import "./loginPage.scss";

export interface LoginPageProps {
  loginState: LoginAPIState;
  handleLogin: (username: string, password: string) => Promise<any>;
}

type Props = LoginPageProps & RouteComponentProps;

const PasswordLoginForm: React.FC<LoginPageProps> = ({
  loginState,
  handleLogin,
}) => {
  // TODO(vilterp): focus username field on mount
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const handleSubmit = (evt: React.FormEvent<any>) => {
    evt.preventDefault();
    handleLogin(username, password);
  };

  return (
    <form
      id="loginForm"
      onSubmit={handleSubmit}
      className="form-internal"
      method="post"
    >
      <TextInput
        name="username"
        onChange={setUsername}
        placeholder="Username"
        label="Username"
        value={username}
      />
      <PasswordInput
        name="password"
        onChange={setPassword}
        placeholder="Password"
        label="Password"
        value={password}
      />
      <Button
        buttonType="submit"
        className="submit-button"
        disabled={loginState.inProgress}
        textAlign={"center"}
      >
        {loginState.inProgress ? "Logging in..." : "Log in"}
      </Button>
    </form>
  );
};

export const LoginPage: React.FC<Props> = ({
  loginState,
  handleLogin,
  location,
  history,
}) => {
  useEffect(() => {
    if (loginState.loggedInUser !== null) {
      const params = new URLSearchParams(location.search);
      if (params.has("redirectTo")) {
        history.push(decodeURIComponent(params.get("redirectTo")));
      } else {
        history.push("/");
      }
    }
  }, [loginState.loggedInUser, location, history]);

  const renderError = () => {
    const { error } = loginState;

    if (!error) {
      return null;
    }

    let message = "Invalid username or password.";
    if (error.message !== "Unauthorized") {
      message = error.message;
    }
    return (
      <div className="login-page__error">
        <img src={ErrorCircle} alt={message} />
        {message}
      </div>
    );
  };

  return (
    <div className="login-page">
      <Helmet title="Login" />
      <div className="login-page__container">
        <CockroachLabsLockupIcon height={37} />
        <div className="content">
          <section className="section login-page__form">
            <div className="form-container">
              <Text textType={TextTypes.Heading2}>
                Log in to the DB Console
              </Text>
              {renderError()}
              <PasswordLoginForm
                loginState={loginState}
                handleLogin={handleLogin}
              />
              <OIDCLoginConnected loginState={loginState} />
              <OIDCGenerateJWTAuthTokenConnected loginState={loginState} />
            </div>
          </section>
          <section className="section login-page__info">
            <Text textType={TextTypes.Heading3}>
              A user with a password is required to log in to the DB Console on
              secure clusters.
            </Text>
            <Text textType={TextTypes.Heading5}>
              Create a user with this SQL command:
            </Text>
            <pre className="login-note-box__sql-command">
              <span className="sql-keyword">CREATE USER</span> craig{" "}
              <span className="sql-keyword">WITH PASSWORD</span>{" "}
              <span className="sql-string">'cockroach'</span>
              <span className="sql-keyword">;</span>
            </pre>
            <p className="aside">
              <a
                href={docsURL.adminUILoginNoVersion}
                className="login-docs-link"
                target="_blank"
                rel="noreferrer"
              >
                <span className="login-docs-link__text">
                  Read more about configuring login
                </span>
              </a>
            </p>
          </section>
        </div>
      </div>
    </div>
  );
};

const LoginPageConnected: React.FC<RouteComponentProps> = ({
  history,
  match,
}) => {
  const dispatch: AppDispatch = useDispatch();
  const loginState = useSelector((state: AdminUIState) => state.login);
  const location = useSelector((state: AdminUIState) => state.router.location);

  const handleLogin = (username: string, password: string) => {
    return dispatch(doLogin(username, password));
  };

  return (
    <LoginPage
      loginState={loginState}
      handleLogin={handleLogin}
      location={location}
      history={history}
      match={match}
    />
  );
};

export default LoginPageConnected;
