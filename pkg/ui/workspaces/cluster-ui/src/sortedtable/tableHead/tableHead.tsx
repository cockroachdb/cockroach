// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React from "react";

import { ExpandableConfig, SortableColumn, SortSetting } from "../sortedtable";

import styles from "./tableHead.module.scss";

const cx = classNames.bind(styles);

interface TableHeadProps {
  columns: SortableColumn[];
  expandableConfig?: ExpandableConfig;
  onChangeSortSetting?: { (ss: SortSetting): void };
  sortSetting?: SortSetting;
  firstCellBordered: boolean;
}

export const TableHead: React.FC<TableHeadProps> = props => {
  const { expandableConfig, columns, sortSetting, firstCellBordered } = props;
  const trClass = cx("head-wrapper__row", "head-wrapper__row--header");
  const thClass = cx("head-wrapper__cell");
  const cellContentWrapper = cx("inner-content-wrapper");
  const arrowsClass = cx("sortable__actions");

  function handleSort(
    newColumnSelected: boolean,
    columnTitle: string,
    prevValue: boolean,
  ) {
    // If the columnTitle is different than the previous value, initial sort
    // descending. If is the same columnTitle the value is updated.

    const ascending = newColumnSelected ? false : !prevValue;
    props.onChangeSortSetting &&
      props.onChangeSortSetting({
        ascending,
        columnTitle,
      });
  }

  return (
    <thead>
      <tr className={trClass}>
        {expandableConfig && <th className={thClass} />}
        {columns.map((c: SortableColumn, idx: number) => {
          const sortable = !!c.columnTitle;
          const newColumnSelected = c.name !== sortSetting.columnTitle;
          const style = { textAlign: c.titleAlign };
          const cellAction = () =>
            sortable &&
            handleSort(newColumnSelected, c.name, sortSetting.ascending);
          const cellClasses = cx(
            "head-wrapper__cell",
            "sorted__cell",
            sortable && "sorted__cell--sortable",
            sortSetting.ascending &&
              !newColumnSelected &&
              "sorted__cell--ascending",
            !sortSetting.ascending &&
              !newColumnSelected &&
              "sorted__cell--descending",
            firstCellBordered && idx === 0 && "cell-header",
          );
          const titleClasses = c.hideTitleUnderline ? "" : cx("column-title");

          return (
            <th
              className={cx(cellClasses)}
              key={"headCell" + idx}
              onClick={_ => cellAction()}
              style={style}
            >
              <div className={cellContentWrapper}>
                <span className={titleClasses}>{c.title} </span>
                {sortable && <span className={arrowsClass} />}
              </div>
            </th>
          );
        })}
      </tr>
    </thead>
  );
};

TableHead.defaultProps = {
  onChangeSortSetting: _ss => {},
};
