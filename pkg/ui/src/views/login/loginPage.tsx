import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { withRouter, WithRouterProps } from "react-router";

import { doLogin, LoginAPIState } from "src/redux/login";
import { AdminUIState } from "src/redux/state";

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
      <div>
        <Helmet>
          <title>Login</title>
        </Helmet>
        <section className="section">
          <h1>Login</h1>
          {this.props.loginState.error
            ? <div className="login-page__error">Login error: {this.props.loginState.error}</div>
            : null}
          <form onSubmit={this.handleSubmit}>
            <input
              type="text"
              onChange={this.handleUpdateUsername}
              value={this.state.username}
            /><br />
            <input
              type="password"
              onChange={this.handleUpdatePassword}
              value={this.state.password}
            /><br />
            <input type="submit" disabled={this.props.loginState.inProgress} />
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
