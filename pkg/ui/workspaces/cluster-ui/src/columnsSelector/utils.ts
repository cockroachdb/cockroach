// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { ColumnDescriptor } from "src";

export const isColumnSelected = (
  selectedColumns: string[] | null | undefined,
  c: ColumnDescriptor<unknown>,
): boolean => {
  return (
    (selectedColumns == null && c.showByDefault !== false) || // show column if list of visible was never defined and can be show by default.
    (selectedColumns !== null && selectedColumns.includes(c.name)) || // show column if user changed its visibility.
    c.alwaysShow === true // show column if alwaysShow option is set explicitly.
  );
};
