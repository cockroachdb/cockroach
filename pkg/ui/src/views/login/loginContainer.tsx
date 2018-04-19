import React from "react";
import { RouterState } from "react-router";

import LoginPage from "src/views/login/loginPage";
import Layout from "src/views/app/containers/layout";

interface LoginContainerState {
  loggedIn: boolean;
}

class LoginContainer extends React.Component<RouterState, LoginContainerState> {
  constructor(props: RouterState) {
    super(props);
    // TODO(vilterp): the user might already have a valid session. how do we check that?
    // maybe do some templating on the backend and add it in.
    this.state = {
      loggedIn: false,
    };
  }

  handleLogin = () => {
    this.setState({
      loggedIn: true,
    });
  }

  render() {
    if (this.state.loggedIn) {
      return (
        <Layout {...this.props}>
          {this.props.children}
        </Layout>
      );
    }

    return <LoginPage onLogin={this.handleLogin} />;
  }
}

export default LoginContainer;
