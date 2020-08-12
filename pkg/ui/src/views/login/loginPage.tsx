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
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { doLogin, LoginAPIState } from "src/redux/login";
import { AdminUIState } from "src/redux/state";
import * as docsURL from "src/util/docs";

import "./loginPage.styl";
import { CockroachLabsLockupIcon, Button, TextInput, PasswordInput } from "src/components";
import { Text, TextTypes } from "src/components";
import ErrorCircle from "assets/error-circle.svg";

export interface LoginPageProps {
  loginState: LoginAPIState;
  handleLogin: (username: string, password: string) => Promise<any>;
}

interface LoginPageState {
  username?: string;
  password?: string;
}

type Props = LoginPageProps & RouteComponentProps;

export class LoginPage extends React.Component<Props, LoginPageState> {
  constructor(props: Props) {
    super(props);
    this.state = {
      username: "",
      password: "",
    };
    // TODO(vilterp): focus username field on mount
  }

  handleUpdateUsername = (value: string) => {
    this.setState({
      username: value,
    });
  }

  handleUpdatePassword = (value: string) => {
    this.setState({
      password: value,
    });
  }

  componentDidUpdate() {
    const { loginState: { loggedInUser } } = this.props;
    if (loggedInUser !== null) {
      const { location, history } = this.props;
      const params = new URLSearchParams(location.search);
      if (params.has("redirectTo")) {
        history.push(params.get("redirectTo"));
      } else {
        history.push("/");
      }
    }
  }

  handleSubmit = (evt: React.FormEvent<any>) => {
    const { handleLogin} = this.props;
    const { username, password } = this.state;
    evt.preventDefault();

    handleLogin(username, password);
  }

  renderError() {
    const { error } = this.props.loginState;

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
        { message }
      </div>
    );
  }

  render() {
    const { username, password } = this.state;
    const { loginState } = this.props;

    return (
      <div className="login-page">
        <Helmet title="Login" />
        <div className="login-page__container">
          <CockroachLabsLockupIcon height={37} />
          <div className="content">
            <section className="section login-page__form">
              <div className="form-container">
                <Text textType={TextTypes.Heading2}>Log in to the Admin UI</Text>
                {this.renderError()}
                <form id="loginForm" onSubmit={this.handleSubmit} className="form-internal" method="post">
                  <TextInput
                    name="username"
                    onChange={this.handleUpdateUsername}
                    placeholder="Username"
                    label="Username"
                    value={username}
                  />
                  <PasswordInput
                    name="password"
                    onChange={this.handleUpdatePassword}
                    placeholder="Password"
                    label="Password"
                    value={password}
                  />
                  <Button buttonType="submit" className="submit-button" disabled={loginState.inProgress} textAlign={"center"}>
                    {loginState.inProgress ? "Logging in..." : "Log in"}
                  </Button>
                </form>
              </div>
            </section>
            <section className="section login-page__info">
              <Text textType={TextTypes.Heading3}>A user with a password is required to log in to the Admin UI on secure clusters.</Text>
              <Text textType={TextTypes.Heading5}>Create a user with this SQL command:</Text>
              <pre className="login-note-box__sql-command">
                <span className="sql-keyword">CREATE USER</span>
                {" "}craig{" "}
                <span className="sql-keyword">WITH PASSWORD</span>
                {" "}
                <span className="sql-string">'cockroach'</span>
                <span className="sql-keyword">;</span>
              </pre>
              <p className="aside">
                <a href={docsURL.adminUILoginNoVersion} className="login-docs-link" target="_blank">
                  <span className="login-docs-link__text">Read more about configuring login</span>
                </a>
              </p>
            </section>
          </div>
        </div>
      </div>
    );
  }
}

// tslint:disable-next-line:variable-name
const LoginPageConnected = withRouter(connect(
  (state: AdminUIState) => {
    return {
      loginState: state.login,
      location: state.router.location,
    };
  },
  (dispatch) => ({
    handleLogin: (username: string, password: string) => {
      return dispatch(doLogin(username, password));
    },
  }),
)(LoginPage));

export default LoginPageConnected;
