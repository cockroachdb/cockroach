// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { connect } from "react-redux";

import { doLogout, LoginState, selectLoginState } from "src/redux/login";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { trustIcon } from "src/util/trust";
import UserMenu from "src/views/app/components/userMenu";
import Popover from "src/views/shared/components/popover";
import UserAvatar from "src/views/shared/components/userAvatar";

import unlockedIcon from "!!raw-loader!assets/unlocked.svg";
import "./loginIndicator.styl";

interface LoginIndicatorProps {
  loginState: LoginState;
  handleLogout: () => void;
}

interface LoginIndicatorState {
  isOpenMenu: boolean;
}

class LoginIndicator extends React.Component<
  LoginIndicatorProps,
  LoginIndicatorState
> {
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
  };

  render() {
    const { loginState, handleLogout } = this.props;
    const { isOpenMenu } = this.state;
    if (!loginState.secureCluster()) {
      return (
        <div className="login-indicator login-indicator--insecure">
          <div
            className="image-container"
            title="Insecure mode"
            dangerouslySetInnerHTML={trustIcon(unlockedIcon)}
          />
          <div className="login-indicator__title">Insecure mode</div>
        </div>
      );
    }

    if (!loginState.displayUserMenu()) {
      return null;
    }

    const user = loginState.loggedInUser();

    if (typeof user == "undefined" || user == null) {
      return null;
    }

    return (
      <div className="login-indicator">
        <Popover
          content={<UserAvatar userName={user} />}
          visible={isOpenMenu}
          onVisibleChange={this.onUserMenuToggle}
        >
          <UserMenu userName={user} onLogoutClick={handleLogout} />
        </Popover>
      </div>
    );
  }
}

export default connect(
  (state: AdminUIState) => ({
    loginState: selectLoginState(state),
  }),
  (dispatch: AppDispatch) => ({
    handleLogout: () => {
      dispatch(doLogout());
    },
  }),
)(LoginIndicator);
