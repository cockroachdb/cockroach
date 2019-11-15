// Copyright 2019 The Cockroach Authors.
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
import { Link } from "react-router";
import { LOGOUT_PAGE } from "src/routes/login";

import "./userMenu.styl";

export interface UserMenuProps {
  userName: string;
  onLogoutClick: () => void;
}

export default function UserMenu (props: UserMenuProps) {
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
