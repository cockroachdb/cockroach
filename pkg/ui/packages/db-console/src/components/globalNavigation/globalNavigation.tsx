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
