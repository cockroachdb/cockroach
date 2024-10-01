// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import * as React from "react";

import "./userAvatar.styl";

export interface UserAvatarProps {
  userName: string;
  disabled?: boolean;
}

export default function UserAvatar(props: UserAvatarProps) {
  const { userName, disabled = false } = props;

  const classes = classNames("user-avatar", {
    "user-avatar--disabled": disabled,
  });

  const nameAbbreviation =
    typeof userName[0] == "undefined" || userName[0] == null
      ? ""
      : userName[0].toUpperCase();

  return (
    <div className={classes}>
      <div>{nameAbbreviation}</div>
    </div>
  );
}
