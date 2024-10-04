// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

import "./pageHeader.styl";

export interface PageHeaderProps {
  children?: React.ReactNode;
}

export function PageHeader(props: PageHeaderProps) {
  return <div className="page-header">{props.children}</div>;
}
