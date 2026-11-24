// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import "./panels.scss";

export function PanelSection({
  children,
}: React.PropsWithChildren<object>): React.ReactElement {
  return (
    <table className="panel-section">
      <tbody>{children}</tbody>
    </table>
  );
}

export function PanelTitle({
  children,
}: React.PropsWithChildren<object>): React.ReactElement {
  return (
    <tr>
      <th colSpan={2} className="panel-title">
        {children}
      </th>
    </tr>
  );
}

export function PanelPair({
  children,
}: React.PropsWithChildren<object>): React.ReactElement {
  return <tr className="panel-pair">{children}</tr>;
}

export function Panel({
  children,
}: React.PropsWithChildren<object>): React.ReactElement {
  return <td className="panel">{children}</td>;
}
