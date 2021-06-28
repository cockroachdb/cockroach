// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames/bind";
import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";
import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";

const pageCx = classNames.bind(statementsPageStyles);
const sortedTableCx = classNames.bind(sortedTableStyles);

export const baseHeadingClasses = {
  wrapper: pageCx("section"),
  tableName: pageCx("base-heading"),
};

export const statisticsClasses = {
  statistic: pageCx("cl-table-statistic"),
  countTitle: pageCx("cl-count-title"),
  lastCleared: pageCx("last-cleared-title"),
  tableContainerClass: sortedTableCx("cl-table-container"),
};
