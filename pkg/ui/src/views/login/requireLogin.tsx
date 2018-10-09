// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

    return (<React.Fragment>{ this.props.children }</React.Fragment>);
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
