// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { connect } from "react-redux";

import { AdminUIState } from "src/redux/state";
import { selectLoginState, LoginState, getLoginPage } from "src/redux/login";

interface RequireLoginProps {
  loginState: LoginState;
}

class RequireLogin extends React.Component<
  RouteComponentProps & RequireLoginProps
> {
  componentDidMount() {
    this.checkLogin();
  }

  componentDidUpdate() {
    this.checkLogin();
  }

  checkLogin() {
    const { location, history } = this.props;

    if (!this.hideLoginPage()) {
      history.push(getLoginPage(location));
    }
  }

  hideLoginPage() {
    return this.props.loginState.hideLoginPage();
  }

  render() {
    if (!this.hideLoginPage()) {
      return null;
    }

    return this.props.children;
  }
}

const RequireLoginConnected = withRouter(
  connect((state: AdminUIState) => {
    return {
      loginState: selectLoginState(state),
    };
  })(RequireLogin),
);

export default RequireLoginConnected;
