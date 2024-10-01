// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

interface RowCellProps {
  cellClasses: string;
  children: string | React.ReactNode;
}

export const RowCell: React.FC<RowCellProps> = ({ cellClasses, children }) => (
  <td className={cellClasses}>{children}</td>
);
