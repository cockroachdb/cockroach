// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import "./infoBox.scss";

export default function InfoBox({
  children,
}: React.PropsWithChildren<object>): React.ReactElement {
  return <div className="info-box">{children}</div>;
}
