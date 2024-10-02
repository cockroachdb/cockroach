// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import cn from "classnames";
import * as React from "react";

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
