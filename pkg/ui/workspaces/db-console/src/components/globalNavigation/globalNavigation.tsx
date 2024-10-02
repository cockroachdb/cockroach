// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

import "./globalNavigation.styl";

export interface GlobalNavigationProps {
  children: React.ReactNode;
}

export interface PanelProps {
  children: React.ReactNode;
}

export function Left(props: PanelProps) {
  return <div className="left-side-panel">{props.children}</div>;
}

export function Right(props: PanelProps) {
  return <div className="right-side-panel">{props.children}</div>;
}

export function GlobalNavigation(props: GlobalNavigationProps) {
  return <div className="global-navigation">{props.children}</div>;
}
