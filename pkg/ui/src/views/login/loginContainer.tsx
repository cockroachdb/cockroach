import React from "react";
import { RouterState } from "react-router";
import { connect } from "react-redux";

import { AdminUIState } from "src/redux/state";
import LoginPage from "src/views/login/loginPage";
import { selectLoginState, LoginState } from "src/redux/login";
import Layout from "src/views/app/containers/layout";

interface LoginContainerProps {
  loginState: LoginState;
}

class LoginContainer extends React.Component<RouterState & LoginContainerProps> {
  render() {
    const { loginState } = this.props;

    if (!loginState.loginEnabled() || loginState.hasAccess()) {
      return (
        <Layout {...this.props}>
          {this.props.children}
        </Layout>
      );
    }

    return <LoginPage />;
  }
}

// tslint:disable-next-line:variable-name
const LoginContainerConnected = connect(
  (state: AdminUIState) => {
    return {
      loginState: selectLoginState(state),
    };
  },
)(LoginContainer);

export default LoginContainerConnected;
