// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Row } from "antd";
import React, { useMemo } from "react";

import {
  StatementFingerprintRequest,
  useStatementFingerprints,
} from "src/api/statementFingerprintsApi";
import { PageLayout, PageSection } from "src/layouts";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import PageCount from "src/sharedFromCloud/pageCount";
import { PageHeader } from "src/sharedFromCloud/pageHeader";
import { Search } from "src/sharedFromCloud/search";
import { Table, TableChangeFn } from "src/sharedFromCloud/table";
import useTable, { TableParams } from "src/sharedFromCloud/useTable";

import { COLUMNS } from "./fingerprintsPageColumns";
import { FingerprintRow } from "./types";
import { statementFingerprintsToRows } from "./utils";

const initialParams = {
  pagination: {
    page: 1,
    pageSize: 10,
  },
  search: "",
};

const createStatementFingerprintRequestFromParams = (
  params: TableParams,
): StatementFingerprintRequest => {
  return {
    pagination: {
      pageSize: params.pagination.pageSize,
      pageNum: params.pagination?.page,
    },
    search: params.search,
  };
};

export const FingerprintsPage = () => {
  const { params, setSearch, setPagination } = useTable({
    initial: initialParams,
  });

  const { data, error, isLoading } = useStatementFingerprints(
    createStatementFingerprintRequestFromParams(params),
  );

  const tableData = useMemo(
    () => statementFingerprintsToRows(data?.results ?? []),
    [data],
  );

  const onTableChange: TableChangeFn<FingerprintRow> = (pagination, _) => {
    setPagination({ page: pagination.current, pageSize: pagination.pageSize });
  };

  const onSearchSubmit = (search: string) => {
    setSearch(search);
    setPagination({ page: 1, pageSize: params.pagination.pageSize });
  };

  return (
    <PageLayout>
      <PageHeader title="Statement Fingerprints" />
      <PageConfig>
        <PageConfigItem>
          <Search placeholder="Search fingerprints" onSubmit={onSearchSubmit} />
        </PageConfigItem>
      </PageConfig>
      <PageSection>
        <Row
          align={"middle"}
          justify={"space-between"}
          style={{ paddingTop: "10px" }}
        >
          <PageCount
            page={params.pagination.page}
            pageSize={params.pagination.pageSize}
            total={data?.pagination.totalResults ?? 0}
            entity="fingerprints"
          />
        </Row>
        <Table
          loading={isLoading}
          error={error}
          columns={COLUMNS}
          dataSource={tableData}
          pagination={{
            size: "small",
            current: params.pagination.page,
            pageSize: params.pagination.pageSize,
            showSizeChanger: false,
            position: ["bottomCenter"],
            total: data?.pagination.totalResults,
          }}
          onChange={onTableChange}
        />
      </PageSection>
    </PageLayout>
  );
};
