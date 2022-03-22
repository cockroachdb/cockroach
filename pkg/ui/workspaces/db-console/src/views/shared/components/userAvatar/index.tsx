// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import classNames from "classnames";

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

  const nameAbbreviation = userName[0].toUpperCase();

  return (
    <div className={classes}>
      <div>{nameAbbreviation}</div>
    </div>
  );
}
