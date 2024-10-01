// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import isEmpty from "lodash/isEmpty";

import { ColumnDescriptor } from "src/sortedtable/sortedtable";

// We show a column if:
// a. The list of visible columns was never defined by (selectedColumns is nullish) and
// the column is set to 'showByDefault'
// b. If the column appears in the selectedColumns list
// c. If the column is labelled as 'alwaysShow'
export const isSelectedColumn = <T>(
  selectedColumns: string[] | null | undefined,
  c: ColumnDescriptor<T>,
): boolean => {
  return (
    (isEmpty(selectedColumns) && c.showByDefault !== false) ||
    (selectedColumns !== null && selectedColumns.includes(c.name)) ||
    c.alwaysShow === true
  );
};
