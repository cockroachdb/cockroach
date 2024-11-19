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
import isUndefined from "lodash/isUndefined";
import moment from "moment-timezone";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link } from "react-router-dom";

import { Anchor, Button, Text, TextTypes, Tooltip } from "src/components";
import { trackCancelDiagnosticsBundleAction } from "src/redux/analyticsActions";
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
} from "src/redux/apiReducers";
import { AdminUIState, AppDispatch } from "src/redux/state";
import { cancelStatementDiagnosticsReportAction } from "src/redux/statements";
import {
  selectStatementDiagnosticsReports,
  selectStatementByFingerprint,
  statementDiagnosticsReportsInFlight,
} from "src/redux/statements/statementsSelectors";
import { trackDownloadDiagnosticsBundle } from "src/util/analytics";
import { statementDiagnostics } from "src/util/docs";
import { summarize } from "src/util/sql/summarize";
import { trustIcon } from "src/util/trust";
import HeaderSection from "src/views/shared/components/headerSection";

import "./statementDiagnosticsHistoryView.styl";

import DownloadIcon from "!!raw-loader!assets/download.svg";
import EmptyTableIcon from "!!url-loader!assets/emptyState/empty-table-results.svg";

type StatementDiagnosticsHistoryViewProps = MapStateToProps &
  MapDispatchToProps;

interface StatementDiagnosticsHistoryViewState {
  sortSetting: {
    columnTitle?: string;
    ascending: boolean;
  };
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

class StatementDiagnosticsHistoryView extends React.Component<
  StatementDiagnosticsHistoryViewProps,
  StatementDiagnosticsHistoryViewState
> {
  columns: ColumnDescriptor<clusterUiApi.StatementDiagnosticsReport>[] = [
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
        const { getStatementByFingerprint } = this.props;
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
                this.props.onDiagnosticCancelRequest(record);
              }}
            >
              Cancel request
            </Button>
          </div>
        );
      },
    },
  ];

  tablePageSize = 16;

  downloadRef = React.createRef<DownloadFileRef>();

  constructor(props: StatementDiagnosticsHistoryViewProps) {
    super(props);
    (this.state = {
      sortSetting: {
        columnTitle: "activated_on",
        ascending: false,
      },
    }),
      props.refresh();
  }

  renderTableTitle = () => {
    const { diagnosticsReports } = this.props;
    const totalCount = diagnosticsReports.length;

    if (totalCount <= this.tablePageSize) {
      return (
        <div className="diagnostics-history-view__table-header">
          <Text>{`${totalCount} diagnostics bundles`}</Text>
        </div>
      );
    }

    return (
      <div className="diagnostics-history-view__table-header">
        <Text>{`${this.tablePageSize} of ${totalCount} diagnostics bundles`}</Text>
      </div>
    );
  };

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });
  };

  render() {
    const { diagnosticsReports, loading } = this.props;
    const dataSource = diagnosticsReports.map((diagnosticsReport, idx) => ({
      ...diagnosticsReport,
      key: idx,
    }));

    return (
      <section className="section">
        <Helmet title="Statement diagnostics history | Debug" />
        <HeaderSection
          title="Statement diagnostics history"
          navigationBackConfig={{
            text: "Advanced Debug",
            path: "/debug",
          }}
        />
        {this.renderTableTitle()}
        <StatementDiagnosticsHistoryTable
          className="statements-table"
          tableWrapperClassName="sorted-table"
          data={dataSource}
          columns={this.columns}
          loading={loading}
          renderNoResult={
            <EmptyTable
              title="No statement diagnostics to show"
              icon={EmptyTableIcon}
              message={
                "Statement diagnostics  can help when troubleshooting issues with specific queries. " +
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
          sortSetting={this.state.sortSetting}
          onChangeSortSetting={this.changeSortSetting}
        />
        <DownloadFile ref={this.downloadRef} />
      </section>
    );
  }
}

interface MapStateToProps {
  loading: boolean;
  diagnosticsReports: clusterUiApi.StatementDiagnosticsReport[];
  getStatementByFingerprint: (
    fingerprint: string,
  ) => ReturnType<typeof selectStatementByFingerprint>;
}

interface MapDispatchToProps {
  onDiagnosticCancelRequest: (
    report: clusterUiApi.StatementDiagnosticsReport,
  ) => void;
  refresh: () => void;
}

const mapStateToProps = (state: AdminUIState): MapStateToProps => ({
  loading: statementDiagnosticsReportsInFlight(state),
  diagnosticsReports: selectStatementDiagnosticsReports(state) || [],
  getStatementByFingerprint: (fingerprint: string) =>
    selectStatementByFingerprint(state, fingerprint),
});

const mapDispatchToProps = (dispatch: AppDispatch): MapDispatchToProps => ({
  onDiagnosticCancelRequest: (
    report: clusterUiApi.StatementDiagnosticsReport,
  ) => {
    dispatch(cancelStatementDiagnosticsReportAction({ requestId: report.id }));
    dispatch(trackCancelDiagnosticsBundleAction(report.statement_fingerprint));
  },
  refresh: () => {
    dispatch(invalidateStatementDiagnosticsRequests());
    dispatch(refreshStatementDiagnosticsRequests());
  },
});

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  StatementDiagnosticsHistoryViewProps,
  AdminUIState
>(
  mapStateToProps,
  mapDispatchToProps,
)(StatementDiagnosticsHistoryView);
