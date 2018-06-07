import classNames from "classnames";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { withRouter, WithRouterProps } from "react-router";

import { doLogin, LoginAPIState } from "src/redux/login";
import { AdminUIState } from "src/redux/state";
import { getDataFromServer } from "src/util/dataFromServer";
import * as docsURL from "src/util/docs";
import { trustIcon } from "src/util/trust";

import logo from "assets/crdb.png";
import docsIcon from "!!raw-loader!assets/docs.svg";

const version = getDataFromServer().Tag || "UNKNOWN";

import "./loginPage.styl";

interface LoginPageProps {
  loginState: LoginAPIState;
  handleLogin: (username: string, password: string) => Promise<void>;
}

interface LoginPageState {
  username: string;
  password: string;
}

class LoginPage extends React.Component<LoginPageProps & WithRouterProps, LoginPageState> {
  constructor(props: LoginPageProps & WithRouterProps) {
    super(props);
    this.state = {
      username: "",
      password: "",
    };
    // TODO(vilterp): focus username field on mount
  }

  handleUpdateUsername = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      username: evt.currentTarget.value,
    });
  }

  handleUpdatePassword = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      password: evt.currentTarget.value,
    });
  }

  handleSubmit = (evt: React.FormEvent<any>) => {
    evt.preventDefault();

    this.props.handleLogin(this.state.username, this.state.password)
        .then(() => {
            const { location, router } = this.props;
            if (location.query && location.query.redirectTo) {
                router.push(location.query.redirectTo);
            } else {
                router.push("/");
            }
        });
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
      <div className="login-page__error">Unable to log in: { message }</div>
    );
  }

  render() {
    const inputClasses = classNames("input-text", {
      "input-text--error": !!this.props.loginState.error,
    });

    return (
      <div className="login-page">
        <Helmet>
          <title>Login</title>
        </Helmet>
        <div className="content">
          <section className="section login-page__info">
            <p className="version">
              Version: <span className="version-tag">{ version }</span>
            </p>
            <img className="logo" alt="CockroachDB" src={logo} />
            <p className="aside">
              Please contact your database administrator for
              account access and password restoration.
            </p>
            <p className="aside">
              <a href={docsURL.adminUIOverview} className="docs-link">
                <span className="docs-link__icon" dangerouslySetInnerHTML={trustIcon(docsIcon)} />
                <span className="docs-link__text">Read the documentation</span>
              </a>
            </p>
          </section>
          <section className="section login-page__form">
            <h1 className="heading">Sign in to the Console</h1>
            {this.renderError()}
            <form onSubmit={this.handleSubmit}>
              <input
                type="text"
                className={inputClasses}
                onChange={this.handleUpdateUsername}
                value={this.state.username}
              />
              <input
                type="password"
                className={inputClasses}
                onChange={this.handleUpdatePassword}
                value={this.state.password}
              />
              <input
                type="submit"
                className="submit-button"
                disabled={this.props.loginState.inProgress}
                value={this.props.loginState.inProgress ? "Signing in..." : "Sign In"}
              />
            </form>
          </section>
        </div>
      </div>
    );
  }
}

// tslint:disable-next-line:variable-name
const LoginPageConnected = connect(
  (state: AdminUIState) => {
    return {
      loginState: state.login,
    };
  },
  (dispatch) => ({
    handleLogin: (username: string, password: string) => {
      return dispatch(doLogin(username, password));
    },
  }),
)(withRouter(LoginPage));

export default LoginPageConnected;
