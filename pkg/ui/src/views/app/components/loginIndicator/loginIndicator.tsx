// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { connect } from "react-redux";

import { AdminUIState } from "src/redux/state";
import { trustIcon } from "src/util/trust";
import Popover from "src/views/shared/components/popover";
import UserAvatar from "src/views/shared/components/userAvatar";
import UserMenu from "src/views/app/components/userMenu";
import { doLogout, LoginState, selectLoginState } from "src/redux/login";

import unlockedIcon from "!!raw-loader!assets/unlocked.svg";
import "./loginIndicator.styl";

interface LoginIndicatorProps {
  loginState: LoginState;
  handleLogout: () => null;
}

interface LoginIndicatorState {
  isOpenMenu: boolean;
}

class LoginIndicator extends React.Component<LoginIndicatorProps, LoginIndicatorState> {
  constructor(props: LoginIndicatorProps) {
    super(props);
    this.state = {
      isOpenMenu: false,
    };
  }

  onUserMenuToggle = (nextState: boolean) => {
    this.setState({
      isOpenMenu: nextState,
    });
  }

  render() {
    const { loginState, handleLogout } = this.props;
    const { isOpenMenu } = this.state;
    if (!loginState.useLogin()) {
      return null;
    }

    if (!loginState.loginEnabled()) {
      return (
        <div className="login-indicator login-indicator--insecure">
          <div>Insecure mode</div>
          <div className="image-container"
               title="Insecure mode"
               dangerouslySetInnerHTML={trustIcon(unlockedIcon)}/>
        </div>
      );
    }

    const user = loginState.loggedInUser();

    if (user == null) {
      return null;
    }

    return (
      <div className="login-indicator">
        <Popover
          content={<UserAvatar userName={user} />}
          visible={isOpenMenu}
          onVisibleChange={this.onUserMenuToggle}
        >
          <UserMenu
            userName={user}
            onLogoutClick={handleLogout}/>
        </Popover>
      </div>
    );
  }
}

export default connect(
  (state: AdminUIState) => ({
    loginState: selectLoginState(state),
  }),
  (dispatch) => ({
    handleLogout: () => {
      dispatch(doLogout());
    },
  }),
)(LoginIndicator);
