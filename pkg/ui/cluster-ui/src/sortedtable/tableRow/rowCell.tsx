// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

interface RowCellProps {
  cellClasses: string;
  children: string | React.ReactNode;
}

export const RowCell: React.FC<RowCellProps> = ({ cellClasses, children }) => (
  <td className={cellClasses}>{children}</td>
);
