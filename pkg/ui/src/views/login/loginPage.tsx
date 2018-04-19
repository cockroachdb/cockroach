import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";

import { doLogin, LoginState } from "src/redux/login";
import { AdminUIState } from "src/redux/state";

interface LoginPageProps {
  loginState: LoginState;
  handleLogin: (username: string, password: string) => void,
}

interface LoginPageState {
  username: string;
  password: string;
}

class LoginPage extends React.Component<LoginPageProps, LoginPageState> {
  constructor(props: LoginPageProps) {
    super(props);
    this.state = {
      username: "",
      password: "",
    };
  }

  handleUpdateUsername = (username: string) => {
    this.setState({
      username,
    });
  }

  handleUpdatePassword = (password: string) => {
    this.setState({
      password,
    });
  }

  handleSubmit = (evt: React.FormEvent<any>) => {
    evt.preventDefault();

    this.props.handleLogin(this.state.username, this.state.password);
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
              onChange={(evt) => this.handleUpdateUsername(evt.target.value)}
              value={this.state.username}
            /><br />
            <input
              type="password"
              onChange={(evt) => this.handleUpdatePassword(evt.target.value)}
              value={this.state.password}
            /><br />
            <input type="submit" disabled={this.props.loginState.inProgress} />
          </form>
        </section>
      </div>
    );
  }
}

const LoginPageConnected = connect(
  (state: AdminUIState) => {
    return {
      loginState: state.login,
    };
  },
  (dispatch) => ({
    handleLogin: (username: string, password: string) => {
      dispatch(doLogin(username, password));
    },
  }),
)(LoginPage);

export default LoginPageConnected;
