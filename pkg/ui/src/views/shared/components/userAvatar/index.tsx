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

import * as React from "react";
import classNames from "classnames";

import "./userAvatar.styl";

export interface UserAvatarProps {
  userName: string;
  disabled?: boolean;
}

export default function UserAvatar(props: UserAvatarProps) {
  const {
    userName,
    disabled = false,
  } = props;

  const classes = classNames("user-avatar", {
    "user-avatar--disabled": disabled,
  });

  const nameAbbreviation = userName[0].toUpperCase();

  return (
    <div className={classes}>
      <div>{nameAbbreviation}</div>
    </div>
  );
}
