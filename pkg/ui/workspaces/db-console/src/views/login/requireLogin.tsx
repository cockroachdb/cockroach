// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
