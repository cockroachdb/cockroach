// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";

import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";
import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";
import statementsTableStyles from "src/statementsTable/statementsTableContent.module.scss";

const sortedTableCx = classNames.bind(sortedTableStyles);
const statementsTableCx = classNames.bind(statementsTableStyles);
const pageCx = classNames.bind(statementsPageStyles);

export const tableClasses = {
  containerClass: sortedTableCx("cl-table-container"),
  latencyClasses: {
    column: statementsTableCx("statements-table__col-latency"),
    barChart: {
      classes: {
        root: statementsTableCx("statements-table__col-latency--bar-chart"),
      },
    },
  },
};

export const statisticsClasses = {
  statistic: pageCx("cl-table-statistic"),
  countTitle: pageCx("cl-count-title"),
  lastCleared: pageCx("last-cleared-title"),
};
