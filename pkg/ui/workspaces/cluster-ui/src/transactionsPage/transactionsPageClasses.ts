// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";

import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";
import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";

import { commonStyles } from "../common";

const pageCx = classNames.bind(statementsPageStyles);
const sortedTableCx = classNames.bind(sortedTableStyles);

export const baseHeadingClasses = {
  wrapper: pageCx("section--heading"),
  tableName: commonStyles("base-heading", "no-margin-bottom"),
};

export const statisticsClasses = {
  statistic: pageCx("cl-table-statistic"),
  countTitle: pageCx("cl-count-title"),
  tableContainerClass: sortedTableCx("cl-table-container"),
};
