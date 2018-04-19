import React from "react";
import Helmet from "react-helmet";

import { userLogin } from "src/util/api";
import { cockroach } from "src/js/protos";
import UserLoginRequest = cockroach.server.serverpb.UserLoginRequest;

interface LoginPageProps {
  onLogin: () => void;
}

interface LoginPageState {
  username: string;
  password: string;
  error: string;
}

class LoginPage extends React.Component<LoginPageProps, LoginPageState> {
  constructor(props: LoginPageProps) {
    super(props);
    this.state = {
      username: "",
      password: "",
      error: null,
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

    const loginReq = new UserLoginRequest({
      username: this.state.username,
      password: this.state.password,
    });
    userLogin(loginReq)
      .then(() => {
        this.props.onLogin();
      })
      .catch((err) => {
        this.setState({
          error: err.toString(),
        });
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
          {this.state.error
            ? <div className="login-page__error">Login error: {this.state.error}</div>
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
            <input type="submit" />
          </form>
        </section>
      </div>
    );
  }
}

export default LoginPage;
