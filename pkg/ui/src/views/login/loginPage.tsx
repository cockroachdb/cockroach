import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { withRouter, WithRouterProps } from "react-router";

import { doLogin, LoginAPIState } from "src/redux/login";
import { AdminUIState } from "src/redux/state";
import docsURL from "src/util/docs";

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

  handleUpdateUsername = (evt: React.FormEvent<any>) => {
    this.setState({
      username: evt.target.value,
    });
  }

  handleUpdatePassword = (evt: React.FormEvent<any>) => {
    this.setState({
      password: evt.target.value,
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

  render() {
    return (
      <div className="login-page">
        <Helmet>
          <title>Login</title>
        </Helmet>
        <section className="section">
          <h1 className="heading">Sign in to the Console</h1>
          <p className="aside">
            Please contact your database administrator for
            account access and password restoration.
            For more information, see{" "}
            <a href={docsURL("admin-ui-overview.html")}>the documentation</a>.
          </p>
          {this.props.loginState.error
            ? <div className="login-page__error">Login error: {this.props.loginState.error}</div>
            : null}
          <form onSubmit={this.handleSubmit}>
            <input
              type="text"
              className="input-text"
              onChange={this.handleUpdateUsername}
              value={this.state.username}
            /><br />
            <input
              type="password"
              className="input-text"
              onChange={this.handleUpdatePassword}
              value={this.state.password}
            /><br />
            <input
              type="submit"
              className="submit-button"
              disabled={this.props.loginState.inProgress}
            />
          </form>
        </section>
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
