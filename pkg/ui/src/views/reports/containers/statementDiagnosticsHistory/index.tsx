// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import moment from "moment";
import { Action, Dispatch } from "redux";
import { Link } from "react-router-dom";
import { isUndefined } from "lodash";

import { Anchor, Button, Text, TextTypes, Tooltip } from "src/components";
import HeaderSection from "src/views/shared/components/headerSection";
import { AdminUIState } from "src/redux/state";
import { trustIcon } from "src/util/trust";
import DownloadIcon from "!!raw-loader!assets/download.svg";
import {
  selectStatementDiagnosticsReports,
  selectStatementByFingerprint,
  statementDiagnosticsReportsInFlight,
} from "src/redux/statements/statementsSelectors";
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
} from "src/redux/apiReducers";
import { DiagnosticStatusBadge } from "src/views/statements/diagnostics/diagnosticStatusBadge";
import "./statementDiagnosticsHistoryView.styl";
import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import {
  SortedTable,
  ColumnDescriptor,
} from "src/views/shared/components/sortedtable";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { statementDiagnostics } from "src/util/docs";
import { summarize } from "src/util/sql/summarize";
import { trackDownloadDiagnosticsBundle } from "src/util/analytics";
import EmptyTableIcon from "!!url-loader!assets/emptyState/empty-table-results.svg";
import {
  DownloadFile,
  DownloadFileRef,
  EmptyTable,
  shortStatement,
  getDiagnosticsStatus,
} from "@cockroachlabs/cluster-ui";

type StatementDiagnosticsHistoryViewProps = MapStateToProps &
  MapDispatchToProps;

interface StatementDiagnosticsHistoryViewState {
  sortSetting: {
    sortKey: number;
    ascending: boolean;
  };
}

class StatementDiagnosticsHistoryTable extends SortedTable<{}> {}

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
  columns: ColumnDescriptor<IStatementDiagnosticsReport>[] = [
    {
      title: "Activated on",
      cell: (record) =>
        moment(record.requested_at.seconds.toNumber() * 1000).format(
          "LL[ at ]h:mm a",
        ),
      sort: (record) => moment(record.requested_at.seconds.toNumber() * 1000),
    },
    {
      title: "Statement",
      cell: (record) => {
        const { getStatementByFingerprint } = this.props;
        const fingerprint = record.statement_fingerprint;
        const statement = getStatementByFingerprint(fingerprint);
        const { implicit_txn: implicitTxn = "true", query } =
          statement?.key?.key_data || {};

        if (isUndefined(query)) {
          return <StatementColumn fingerprint={fingerprint} />;
        }

        return (
          <Link
            to={`/statement/${implicitTxn}/${encodeURIComponent(query)}`}
            className="crl-statements-diagnostics-view__statements-link"
          >
            <StatementColumn fingerprint={fingerprint} />
          </Link>
        );
      },
      sort: (record) => record.statement_fingerprint,
    },
    {
      title: "Status",
      sort: (record) => `${record.completed}`,
      cell: (record) => (
        <Text>
          <DiagnosticStatusBadge status={getDiagnosticsStatus(record)} />
        </Text>
      ),
    },
    {
      title: "",
      cell: (record) => {
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
        return null;
      },
    },
  ];

  tablePageSize = 16;

  downloadRef = React.createRef<DownloadFileRef>();

  constructor(props: StatementDiagnosticsHistoryViewProps) {
    super(props);
    (this.state = {
      sortSetting: {
        sortKey: 0,
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
          <Text>{`${totalCount} traces`}</Text>
        </div>
      );
    }

    return (
      <div className="diagnostics-history-view__table-header">
        <Text>{`${this.tablePageSize} of ${totalCount} traces`}</Text>
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
  diagnosticsReports: IStatementDiagnosticsReport[];
  getStatementByFingerprint: (
    fingerprint: string,
  ) => ReturnType<typeof selectStatementByFingerprint>;
}

interface MapDispatchToProps {
  refresh: () => void;
}

const mapStateToProps = (state: AdminUIState): MapStateToProps => ({
  loading: statementDiagnosticsReportsInFlight(state),
  diagnosticsReports: selectStatementDiagnosticsReports(state) || [],
  getStatementByFingerprint: (fingerprint: string) =>
    selectStatementByFingerprint(state, fingerprint),
});

const mapDispatchToProps = (
  dispatch: Dispatch<Action, AdminUIState>,
): MapDispatchToProps => ({
  refresh: () => {
    dispatch(invalidateStatementDiagnosticsRequests());
    dispatch(refreshStatementDiagnosticsRequests());
  },
});

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  StatementDiagnosticsHistoryViewProps
>(
  mapStateToProps,
  mapDispatchToProps,
)(StatementDiagnosticsHistoryView);
