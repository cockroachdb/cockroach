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
import { withRouter, WithRouterProps } from "react-router";
import { connect } from "react-redux";

import { AdminUIState } from "src/redux/state";
import { selectLoginState, LoginState, getLoginPage } from "src/redux/login";

interface RequireLoginProps {
  loginState: LoginState;
}

class RequireLogin extends React.Component<WithRouterProps & RequireLoginProps> {
  componentWillMount() {
    this.checkLogin();
  }

  componentWillReceiveProps() {
    this.checkLogin();
  }

  checkLogin() {
    const { location, router } = this.props;

    if (!this.hasAccess()) {
      router.push(getLoginPage(location));
    }
  }

  hasAccess() {
    return this.props.loginState.hasAccess();
  }

  render() {
    if (!this.hasAccess()) {
      return null;
    }

    return this.props.children;
  }
}

// tslint:disable-next-line:variable-name
const RequireLoginConnected = connect(
  (state: AdminUIState) => {
    return {
      loginState: selectLoginState(state),
    };
  },
)(withRouter(RequireLogin));

export default RequireLoginConnected;
