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
import cn from "classnames";

import { Text, TextTypes } from "src/components";

import "./sideNavigation.styl";

export interface SideNavigationProps {
  children: React.ReactNode;
  className?: string;
}

export interface NavigationItem {
  disabled?: boolean;
  isActive?: boolean;
  className?: string;
  children: React.ReactNode;
}

export function NavigationItem(props: NavigationItem) {
  const { children, isActive, disabled } = props;
  let textType = TextTypes.Body;

  if (isActive) {
    textType = TextTypes.BodyStrong;
  }

  const classes = cn("side-navigation__navigation-item", {
    "side-navigation__navigation-item--active": isActive,
    "side-navigation__navigation-item--disabled": disabled,
  });

  return (
    <li className={classes}>
      <Text textType={textType}>{children}</Text>
    </li>
  );
}

NavigationItem.defaultProps = {
  isActive: false,
  disabled: false,
};

SideNavigation.Item = NavigationItem;

export function SideNavigation(props: SideNavigationProps) {
  return (
    <nav className="side-navigation">
      <ul className="side-navigation--list">{props.children}</ul>
    </nav>
  );
}
