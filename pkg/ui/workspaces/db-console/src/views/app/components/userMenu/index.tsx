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
import { Link } from "react-router-dom";
import { LOGOUT_PAGE } from "src/routes/login";

import "./userMenu.styl";

export interface UserMenuProps {
  userName: string;
  onLogoutClick: () => void;
}

export default function UserMenu(props: UserMenuProps) {
  const { userName, onLogoutClick } = props;
  return (
    <div className="user-menu">
      <div className="user-menu__item user-menu__username">{userName}</div>
      <div className="user-menu__item user-menu__logout-menu-item">
        <Link to={LOGOUT_PAGE} onClick={onLogoutClick}>
          Logout
        </Link>
      </div>
    </div>
  );
}
