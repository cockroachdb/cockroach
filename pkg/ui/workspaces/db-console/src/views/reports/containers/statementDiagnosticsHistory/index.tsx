// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  api as clusterUiApi,
  DownloadFile,
  DownloadFileRef,
  EmptyTable,
  shortStatement,
  getDiagnosticsStatus,
  DiagnosticStatusBadge,
  SortedTable,
  SortSetting,
  ColumnDescriptor,
  util,
  Timestamp,
} from "@cockroachlabs/cluster-ui";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import isUndefined from "lodash/isUndefined";
import moment from "moment-timezone";
import React, { useRef, useState, useCallback } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Link } from "react-router-dom";
import useSWR from "swr";

import { Anchor, Button, Text, TextTypes, Tooltip } from "src/components";
import {
  createStatementDiagnosticsAlertLocalSetting,
  cancelStatementDiagnosticsAlertLocalSetting,
} from "src/redux/alerts";
import { trackCancelDiagnosticsBundleAction } from "src/redux/analyticsActions";
import { AdminUIState } from "src/redux/state";
import { trackDownloadDiagnosticsBundle } from "src/util/analytics";
import { statementDiagnostics } from "src/util/docs";
import { summarize } from "src/util/sql/summarize";
import { trustIcon } from "src/util/trust";

import "./statementDiagnosticsHistoryView.styl";

import DownloadIcon from "!!raw-loader!assets/download.svg";
import EmptyTableIcon from "!!url-loader!assets/emptyState/empty-table-results.svg";

export type Stmt =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;

const DIAGNOSTICS_REPORTS_KEY = "statement-diagnostics-reports";

function useStatementDiagnosticsReports() {
  const {
    data,
    error,
    isLoading,
    mutate: mutateDiagnostics,
  } = useSWR(
    DIAGNOSTICS_REPORTS_KEY,
    () => clusterUiApi.getStatementDiagnosticsReports(),
    {
      // Poll every 30 seconds if there are active (non-completed) reports
      refreshInterval: latestData => {
        if (!latestData) return 0;
        const hasActiveRequests = latestData.some(report => !report.completed);
        return hasActiveRequests ? 30000 : 0;
      },
      revalidateOnFocus: true,
    },
  );

  return {
    data: data || [],
    loading: isLoading,
    error,
    refresh: mutateDiagnostics,
  };
}

interface StatementDiagnosticsHistoryViewProps {
  diagnosticsReports: clusterUiApi.StatementDiagnosticsReport[];
  loading: boolean;
  getStatementByFingerprint: (fingerprint: string) => Stmt | undefined;
  onCancelRequest: (
    report: clusterUiApi.StatementDiagnosticsReport,
  ) => Promise<void>;
}

class StatementDiagnosticsHistoryTable extends SortedTable<clusterUiApi.StatementDiagnosticsReport> {}

const StatementColumn: React.FC<{ fingerprint: string }> = ({
  fingerprint,
}) => {
  const summary = summarize(fingerprint);
  const shortenedStatement = shortStatement(summary, fingerprint);
  const showTooltip = fingerprint !== shortenedStatement;

  if (showTooltip) {
    return (
      <Text textType={TextTypes.Code}>
        <Tooltip
          placement="bottom"
          title={
            <pre className="cl-table-link__description">{fingerprint}</pre>
          }
          overlayClassName="cl-table-link__statement-tooltip--fixed-width"
        >
          {shortenedStatement}
        </Tooltip>
      </Text>
    );
  }
  return <Text textType={TextTypes.Code}>{shortenedStatement}</Text>;
};

export const StatementDiagnosticsHistoryView: React.FC<
  StatementDiagnosticsHistoryViewProps
> = ({
  diagnosticsReports,
  loading,
  getStatementByFingerprint,
  onCancelRequest,
}) => {
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    columnTitle: "activated_on",
    ascending: false,
  });

  const downloadRef = useRef<DownloadFileRef>();
  const tablePageSize = 16;

  const columns: ColumnDescriptor<clusterUiApi.StatementDiagnosticsReport>[] = [
    {
      title: "Activated on",
      name: "activated_on",
      cell: record => (
        <Timestamp time={record.requested_at} format={util.DATE_FORMAT_24_TZ} />
      ),
      sort: record => {
        return moment.utc(record.requested_at).unix();
      },
    },
    {
      title: "Statement",
      name: "statement",
      cell: record => {
        const fingerprint = record.statement_fingerprint;
        const statement = getStatementByFingerprint(fingerprint);
        const { implicit_txn: implicitTxn = "true", query } =
          statement?.key?.key_data || {};

        if (isUndefined(query)) {
          return <StatementColumn fingerprint={fingerprint} />;
        }

        const base = `/statement/${implicitTxn}`;
        const statementFingerprintID = statement.id.toString();
        const path = `${base}/${encodeURIComponent(statementFingerprintID)}`;

        return (
          <Link
            to={path}
            className="crl-statements-diagnostics-view__statements-link"
          >
            <StatementColumn fingerprint={fingerprint} />
          </Link>
        );
      },
      sort: record => record.statement_fingerprint,
    },
    {
      title: "Status",
      name: "status",
      sort: record => `${record.completed}`,
      cell: record => (
        <Text>
          <DiagnosticStatusBadge status={getDiagnosticsStatus(record)} />
        </Text>
      ),
    },
    {
      title: "",
      name: "actions",
      cell: record => {
        if (record.completed) {
          return (
            <div className="crl-statements-diagnostics-view__actions-column cell--show-on-hover nodes-table__link">
              <a
                href={`_admin/v1/stmtbundle/${record.statement_diagnostics_id}`}
                onClick={() =>
                  trackDownloadDiagnosticsBundle(record.statement_fingerprint)
                }
              >
                <Button
                  size="small"
                  type="flat"
                  iconPosition="left"
                  icon={() => (
                    <span
                      className="crl-statements-diagnostics-view__icon"
                      dangerouslySetInnerHTML={trustIcon(DownloadIcon)}
                    />
                  )}
                >
                  Bundle (.zip)
                </Button>
              </a>
            </div>
          );
        }
        return (
          <div className="crl-statements-diagnostics-view__actions-column cell--show-on-hover nodes-table__link">
            <Button
              size="small"
              type="secondary"
              onClick={() => {
                onCancelRequest(record);
              }}
            >
              Cancel request
            </Button>
          </div>
        );
      },
    },
  ];

  const renderTableTitle = () => {
    const totalCount = diagnosticsReports.length;

    if (totalCount <= tablePageSize) {
      return (
        <div className="diagnostics-history-view__table-header">
          <Text>{`${totalCount} diagnostics bundles`}</Text>
        </div>
      );
    }

    return (
      <div className="diagnostics-history-view__table-header">
        <Text>{`${tablePageSize} of ${totalCount} diagnostics bundles`}</Text>
      </div>
    );
  };

  const changeSortSetting = (ss: SortSetting) => {
    setSortSetting(ss);
  };

  const dataSource = diagnosticsReports.map((diagnosticsReport, idx) => ({
    ...diagnosticsReport,
    key: idx,
  }));

  return (
    <>
      {renderTableTitle()}
      <StatementDiagnosticsHistoryTable
        className="statements-table"
        tableWrapperClassName="sorted-table"
        data={dataSource}
        columns={columns}
        loading={loading}
        renderNoResult={
          <EmptyTable
            title="No statement diagnostics to show"
            icon={EmptyTableIcon}
            message={
              "Statement diagnostics can help when troubleshooting issues with specific queries. " +
              "The diagnostic bundle can be activated from individual statement pages and will include EXPLAIN" +
              " plans, table statistics, and traces."
            }
            footer={
              <Anchor href={statementDiagnostics} target="_blank">
                Learn more about statement diagnostics
              </Anchor>
            }
          />
        }
        sortSetting={sortSetting}
        onChangeSortSetting={changeSortSetting}
      />
      <DownloadFile ref={downloadRef} />
    </>
  );
};

const StatementDiagnosticsHistoryContainer: React.FC = () => {
  const dispatch = useDispatch();
  const {
    data: diagnosticsReports,
    loading,
    refresh,
  } = useStatementDiagnosticsReports();

  const statements = useSelector(
    (state: AdminUIState) => state.cachedData.statements.data?.statements,
  );

  const getStatementByFingerprint = useCallback(
    (fingerprint: string): Stmt | undefined => {
      return (statements || []).find(
        statement => statement.key.key_data.query === fingerprint,
      );
    },
    [statements],
  );

  const handleCancelRequest = useCallback(
    async (report: clusterUiApi.StatementDiagnosticsReport) => {
      try {
        await clusterUiApi.cancelStatementDiagnosticsReport({
          requestId: report.id,
        });

        dispatch(
          trackCancelDiagnosticsBundleAction(report.statement_fingerprint),
        );

        dispatch(
          createStatementDiagnosticsAlertLocalSetting.set({
            show: false,
          }),
        );
        dispatch(
          cancelStatementDiagnosticsAlertLocalSetting.set({
            show: true,
            status: "SUCCESS",
          }),
        );

        await refresh();
      } catch (error) {
        dispatch(
          createStatementDiagnosticsAlertLocalSetting.set({
            show: false,
          }),
        );
        dispatch(
          cancelStatementDiagnosticsAlertLocalSetting.set({
            show: true,
            status: "FAILED",
          }),
        );
      }
    },
    [dispatch, refresh],
  );

  return (
    <StatementDiagnosticsHistoryView
      diagnosticsReports={diagnosticsReports}
      loading={loading}
      getStatementByFingerprint={getStatementByFingerprint}
      onCancelRequest={handleCancelRequest}
    />
  );
};

export default StatementDiagnosticsHistoryContainer;
