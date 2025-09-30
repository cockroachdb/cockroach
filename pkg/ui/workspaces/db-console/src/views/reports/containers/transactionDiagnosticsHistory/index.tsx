// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  api as clusterUiApi,
  DownloadFile,
  DownloadFileRef,
  EmptyTable,
  getDiagnosticsStatus,
  DiagnosticStatusBadge,
  SortedTable,
  SortSetting,
  ColumnDescriptor,
  util,
  Timestamp,
} from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import React, { useRef, useState, useCallback } from "react";
import { useDispatch } from "react-redux";
import { Link } from "react-router-dom";
import useSWR from "swr";

import { Anchor, Button, Text, TextTypes, Tooltip } from "src/components";
import {
  createStatementDiagnosticsAlertLocalSetting,
  cancelStatementDiagnosticsAlertLocalSetting,
} from "src/redux/alerts";
import { trackCancelDiagnosticsBundleAction } from "src/redux/analyticsActions";
import { trackDownloadDiagnosticsBundle } from "src/util/analytics";
import { statementDiagnostics } from "src/util/docs";
import { trustIcon } from "src/util/trust";

import "./transactionDiagnosticsHistoryView.styl";

import DownloadIcon from "!!raw-loader!assets/download.svg";
import EmptyTableIcon from "!!url-loader!assets/emptyState/empty-table-results.svg";

const TRANSACTION_DIAGNOSTICS_REPORTS_KEY = "transaction-diagnostics-reports";

function useTransactionDiagnosticsReports() {
  const {
    data,
    error,
    isLoading,
    mutate: mutateDiagnostics,
  } = useSWR(
    TRANSACTION_DIAGNOSTICS_REPORTS_KEY,
    () => clusterUiApi.getTransactionDiagnosticsReports(),
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

interface TransactionDiagnosticsHistoryViewProps {
  diagnosticsReports: clusterUiApi.TransactionDiagnosticsReport[];
  loading: boolean;
  onCancelRequest: (
    report: clusterUiApi.TransactionDiagnosticsReport,
  ) => Promise<void>;
}

class TransactionDiagnosticsHistoryTable extends SortedTable<clusterUiApi.TransactionDiagnosticsReport> {}

const TransactionColumn: React.FC<{ fingerprint: string }> = ({
  fingerprint,
}) => {
  const shortenedFingerprint =
    fingerprint.length > 50
      ? `${fingerprint.substring(0, 47)}...`
      : fingerprint;
  const showTooltip = fingerprint !== shortenedFingerprint;

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
          {shortenedFingerprint}
        </Tooltip>
      </Text>
    );
  }
  return <Text textType={TextTypes.Code}>{shortenedFingerprint}</Text>;
};

export const TransactionDiagnosticsHistoryView: React.FC<
  TransactionDiagnosticsHistoryViewProps
> = ({ diagnosticsReports, loading, onCancelRequest }) => {
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    columnTitle: "activated_on",
    ascending: false,
  });

  const downloadRef = useRef<DownloadFileRef>();
  const tablePageSize = 16;

  const columns: ColumnDescriptor<clusterUiApi.TransactionDiagnosticsReport>[] =
    [
      {
        title: "Activated on",
        name: "activated_on",
        cell: record => (
          <Timestamp
            time={record.requested_at}
            format={util.DATE_FORMAT_24_TZ}
          />
        ),
        sort: record => {
          return moment.utc(record.requested_at).unix();
        },
      },
      {
        title: "Transaction",
        name: "transaction",
        cell: record => {
          const displayText =
            record.transaction_fingerprint ||
            `Transaction ${record.transaction_fingerprint_id}`;

          const transactionLink = `/transaction/${record.transaction_fingerprint_id}`;

          return (
            <Link
              to={transactionLink}
              className="crl-statements-diagnostics-view__statements-link"
            >
              <TransactionColumn fingerprint={displayText} />
            </Link>
          );
        },
        sort: record => record.transaction_fingerprint_id.toString(),
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
                  href={`_admin/v1/txnbundle/${record.transaction_diagnostics_id}`}
                  onClick={() =>
                    trackDownloadDiagnosticsBundle(
                      record.transaction_fingerprint,
                    )
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
      <TransactionDiagnosticsHistoryTable
        className="statements-table"
        tableWrapperClassName="sorted-table"
        data={dataSource}
        columns={columns}
        loading={loading}
        renderNoResult={
          <EmptyTable
            title="No transaction diagnostics to show"
            icon={EmptyTableIcon}
            message={
              "Transaction diagnostics can help when troubleshooting issues with specific transactions. " +
              "The diagnostic bundle can be activated from individual transaction pages and will include EXPLAIN" +
              " plans, table statistics, and traces for all statements in the transaction."
            }
            footer={
              <Anchor href={statementDiagnostics} target="_blank">
                Learn more about transaction diagnostics
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

const TransactionDiagnosticsHistoryContainer: React.FC = () => {
  const dispatch = useDispatch();
  const {
    data: diagnosticsReports,
    loading,
    refresh,
  } = useTransactionDiagnosticsReports();

  const handleCancelRequest = useCallback(
    async (report: clusterUiApi.TransactionDiagnosticsReport) => {
      try {
        await clusterUiApi.cancelTransactionDiagnosticsReport({
          requestId: report.id,
        });

        dispatch(
          trackCancelDiagnosticsBundleAction(report.transaction_fingerprint),
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
    <TransactionDiagnosticsHistoryView
      diagnosticsReports={diagnosticsReports}
      loading={loading}
      onCancelRequest={handleCancelRequest}
    />
  );
};

export default TransactionDiagnosticsHistoryContainer;
