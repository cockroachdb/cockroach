// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState } from "react";
import { useDispatch, useSelector } from "react-redux";

import { doLogout, selectLoginState } from "src/redux/login";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { trustIcon } from "src/util/trust";
import UserMenu from "src/views/app/components/userMenu";
import Popover from "src/views/shared/components/popover";
import UserAvatar from "src/views/shared/components/userAvatar";

import unlockedIcon from "!!raw-loader!assets/unlocked.svg";
import "./loginIndicator.scss";

function LoginIndicator(): React.ReactElement {
  const loginState = useSelector((state: AdminUIState) =>
    selectLoginState(state),
  );
  const dispatch: AppDispatch = useDispatch();
  const [isOpenMenu, setIsOpenMenu] = useState(false);

  const handleLogout = () => {
    dispatch(doLogout());
  };

  const onUserMenuToggle = (nextState: boolean) => {
    setIsOpenMenu(nextState);
  };

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
        onVisibleChange={onUserMenuToggle}
      >
        <UserMenu userName={user} onLogoutClick={handleLogout} />
      </Popover>
    </div>
  );
}

export default LoginIndicator;
