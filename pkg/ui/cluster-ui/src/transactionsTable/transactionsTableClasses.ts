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
  RowsAffectedClasses: {
    column: statementsTableCx("statements-table__col-rows"),
    barChart: {
      classes: {
        root: statementsTableCx("statements-table__col-rows--bar-chart"),
        label: statementsTableCx(
          "statements-table__col-rows--bar-chart__label",
        ),
      },
    },
  },
};

export const statisticsClasses = {
  statistic: pageCx("cl-table-statistic"),
  countTitle: pageCx("cl-count-title"),
  lastCleared: pageCx("last-cleared-title"),
};
