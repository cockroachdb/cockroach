// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo, useState, useEffect } from "react";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import classNames from "classnames/bind";
import { Pagination } from "src/pagination";
import { SortSetting } from "src/sortedtable";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import { ActivateDiagnosticsModalRef } from "src/statementsDiagnostics";
import { calculateTotalWorkload, syncHistory, unique } from "src/util";
import ColumnsSelector from "../columnsSelector/columnsSelector";
import { SelectOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import { calculateActiveFilters, Filters } from "../queryFilter";
import { ISortedTablePagination } from "../sortedtable";
import {
  AggregateStatistics,
  makeStatementsColumns,
  populateRegionNodeForStatements,
  StatementsSortedTable,
} from "../statementsTable";
import {
  getLabel,
  StatisticTableColumnKeys,
} from "../statsTableUtil/statsTableUtil";
import { UIConfigState } from "../store";
import { TableStatistics } from "../tableStatistics";
import { EmptyStatementsPlaceholder } from "./emptyStatementsPlaceholder";
import { useHistory } from "react-router-dom";
import { filterStatements, isColumnSelected } from "./utils";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

const sortableTableCx = classNames.bind(sortableTableStyles);

type StatementsTableWrapperProps = {
  statements: AggregateStatistics[];
  onSelectDiagnosticsReportDropdownOption?: (
    report: IStatementDiagnosticsReport,
  ) => void;
  onStatementClick?: (statement: string) => void;
  columns: string[];
  onColumnsChange?: (selectedColumns: string[]) => void;
  nodeRegions: { [key: string]: string };
  isTenant?: UIConfigState["isTenant"];
  hasViewActivityRedactedRole?: UIConfigState["hasViewActivityRedactedRole"];
  sortSetting: SortSetting;
  search: string;
  filters: Filters;
  activateDiagnosticsRef?: React.RefObject<ActivateDiagnosticsModalRef>;
  onClearFilters: () => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onPageChanged?: (newPage: number) => void;
};

const StatementsTableWrapper = ({
  activateDiagnosticsRef,
  columns: userSelectedColumnsToShow,
  filters,
  hasViewActivityRedactedRole,
  isTenant,
  nodeRegions,
  search,
  sortSetting,
  statements,
  onClearFilters,
  onColumnsChange,
  onPageChanged,
  onSelectDiagnosticsReportDropdownOption,
  onSortingChange,
  onStatementClick,
}: StatementsTableWrapperProps): React.ReactElement => {
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    pageSize: 20,
    current: 1,
  });
  const history = useHistory();

  useEffect(() => {
    setPagination({
      pageSize: 20,
      current: 1,
    });
  }, [search, filters]);

  const nodes = isTenant
    ? []
    : Object.keys(nodeRegions)
        .map(n => Number(n))
        .sort();
  const regions = isTenant
    ? []
    : unique(nodes.map(node => nodeRegions[node.toString()])).sort();

  // If the cluster is a tenant cluster we don't show info
  // about nodes/regions.
  populateRegionNodeForStatements(statements, nodeRegions, isTenant);

  const data = useMemo((): AggregateStatistics[] => {
    if (statements == null) return [];
    return filterStatements(statements, filters, search, isTenant, nodeRegions);
  }, [filters, isTenant, nodeRegions, search, statements]);

  const totalWorkload = calculateTotalWorkload(data);
  const totalCount = data.length;
  const isEmptySearchResults = data?.length > 0 && search?.length > 0;

  // Creates a list of all possible columns,
  // hiding nodeRegions if is not multi-region and
  // hiding columns that won't be displayed for tenants.
  const columns = makeStatementsColumns(
    statements,
    filters.app,
    totalWorkload,
    nodeRegions,
    "statement",
    isTenant,
    hasViewActivityRedactedRole,
    search,
    activateDiagnosticsRef,
    onSelectDiagnosticsReportDropdownOption,
    onStatementClick,
  )
    .filter(c => !(c.name === "regionNodes" && regions.length < 2))
    .filter(c => !(isTenant && c.hideIfTenant));

  // Iterate over all available columns and create list of SelectOptions with initial selection
  // values based on stored user selections in local storage and default column configs.
  // Columns that are set to alwaysShow are filtered from the list.
  const tableColumns = columns
    .filter(c => !c.alwaysShow)
    .map(
      (c): SelectOption => ({
        label: getLabel(c.name as StatisticTableColumnKeys, "statement"),
        value: c.name,
        isSelected: isColumnSelected(c, userSelectedColumnsToShow),
      }),
    );

  // List of all columns that will be displayed based on the column selection.
  const displayColumns = columns.filter(c =>
    isColumnSelected(c, userSelectedColumnsToShow),
  );

  const activeFilters = calculateActiveFilters(filters);

  const changeSortSetting = (ss: SortSetting): void => {
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      history,
    );
    if (onSortingChange) {
      onSortingChange("Statements", ss.columnTitle, ss.ascending);
    }
  };

  const onChangePage = (current: number): void => {
    setPagination({ ...pagination, current });
    if (onPageChanged) {
      onPageChanged(current);
    }
  };

  return (
    <div>
      <section className={sortableTableCx("cl-table-container")}>
        <div>
          <ColumnsSelector
            options={tableColumns}
            onSubmitColumns={onColumnsChange}
          />
          <TableStatistics
            pagination={pagination}
            search={search}
            totalCount={totalCount}
            arrayItemName="statements"
            activeFilters={activeFilters}
            onClearFilters={onClearFilters}
          />
        </div>
        <StatementsSortedTable
          className="statements-table"
          data={data}
          columns={displayColumns}
          sortSetting={sortSetting}
          onChangeSortSetting={changeSortSetting}
          renderNoResult={
            <EmptyStatementsPlaceholder
              isEmptySearchResults={isEmptySearchResults}
            />
          }
          pagination={pagination}
        />
      </section>
      <Pagination
        pageSize={pagination.pageSize}
        current={pagination.current}
        total={data.length}
        onChange={onChangePage}
      />
    </div>
  );
};

export default React.memo(StatementsTableWrapper);
