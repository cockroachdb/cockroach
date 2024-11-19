// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
