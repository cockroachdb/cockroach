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
import React, { useEffect, useRef, useState } from "react";
import { connect } from "react-redux";
import { Link } from "react-router-dom";

import { Anchor, Button, Text, TextTypes, Tooltip } from "src/components";
import { trackCancelDiagnosticsBundleAction } from "src/redux/analyticsActions";
import {
  invalidateTransactionDiagnosticsRequests,
  refreshTransactionDiagnosticsRequests,
} from "src/redux/apiReducers";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { cancelTransactionDiagnosticsReportAction } from "src/redux/statements";
import {
  selectTransactionDiagnosticsReports,
  transactionDiagnosticsReportsInFlight,
} from "src/redux/statements/statementsSelectors";
import { trackDownloadDiagnosticsBundle } from "src/util/analytics";
import { statementDiagnostics } from "src/util/docs";
import { trustIcon } from "src/util/trust";

import "./transactionDiagnosticsHistoryView.styl";

import DownloadIcon from "!!raw-loader!assets/download.svg";
import EmptyTableIcon from "!!url-loader!assets/emptyState/empty-table-results.svg";

type TransactionDiagnosticsHistoryViewProps = MapStateToProps &
  MapDispatchToProps;

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

const TransactionDiagnosticsHistoryView: React.FC<
  TransactionDiagnosticsHistoryViewProps
> = ({ diagnosticsReports, loading, onDiagnosticCancelRequest, refresh }) => {
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    columnTitle: "activated_on",
    ascending: false,
  });

  const downloadRef = useRef<DownloadFileRef>();
  const tablePageSize = 16;

  useEffect(() => {
    refresh();
  }, [refresh]);

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
          let txnId = BigInt(0);
          if (
            record.transaction_fingerprint_id &&
            record.transaction_fingerprint_id.length >= 8
          ) {
            // Note: the probuf code constructs the Uint8Array fingeprint
            // using a shared underlying buffer so we need to use its
            // specific offset when decoding the Uint64 within.
            const dv = new DataView(record.transaction_fingerprint_id.buffer);
            txnId = dv.getBigUint64(
              record.transaction_fingerprint_id.byteOffset,
            );
          }

          const displayText =
            record.transaction_fingerprint || `Transaction ${txnId}`;

          // Link to transaction details page
          const transactionLink = `/transaction/${txnId}`;

          return (
            <Link
              to={transactionLink}
              className="crl-statements-diagnostics-view__statements-link"
            >
              <TransactionColumn fingerprint={displayText} />
            </Link>
          );
        },
        sort: record => record.transaction_fingerprint_id.join(),
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
                  onDiagnosticCancelRequest(record);
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

interface MapStateToProps {
  loading: boolean;
  diagnosticsReports: clusterUiApi.TransactionDiagnosticsReport[];
}

interface MapDispatchToProps {
  onDiagnosticCancelRequest: (
    report: clusterUiApi.TransactionDiagnosticsReport,
  ) => void;
  refresh: () => void;
}

const mapStateToProps = (state: AdminUIState): MapStateToProps => ({
  loading: transactionDiagnosticsReportsInFlight(state),
  diagnosticsReports: selectTransactionDiagnosticsReports(state) || [],
});

const mapDispatchToProps = (dispatch: AppDispatch): MapDispatchToProps => ({
  onDiagnosticCancelRequest: (
    report: clusterUiApi.TransactionDiagnosticsReport,
  ) => {
    dispatch(
      cancelTransactionDiagnosticsReportAction({ requestId: report.id }),
    );
    dispatch(
      trackCancelDiagnosticsBundleAction(report.transaction_fingerprint),
    );
  },
  refresh: () => {
    dispatch(invalidateTransactionDiagnosticsRequests());
    dispatch(refreshTransactionDiagnosticsRequests());
  },
});

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  TransactionDiagnosticsHistoryViewProps,
  AdminUIState
>(
  mapStateToProps,
  mapDispatchToProps,
)(TransactionDiagnosticsHistoryView);
