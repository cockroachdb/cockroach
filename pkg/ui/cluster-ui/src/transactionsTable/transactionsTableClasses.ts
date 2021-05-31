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
import statementsTableStyles from "src/statementsTable/statementsTableContent.module.scss";
import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";

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
