// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { isEmpty } from "lodash";
import { ColumnDescriptor } from "src/sortedtable/sortedtable";

// We show a column if:
// a. The list of visible columns was never defined by (selectedColumns is nullish) and
// the column is set to 'showByDefault'
// b. If the column appears in the selectedColumns list
// c. If the column is labelled as 'alwaysShow'
export const isSelectedColumn = (
  selectedColumns: string[] | null | undefined,
  c: ColumnDescriptor<unknown>,
): boolean => {
  return (
    (isEmpty(selectedColumns) && c.showByDefault !== false) ||
    (selectedColumns !== null && selectedColumns.includes(c.name)) ||
    c.alwaysShow === true
  );
};
