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
import Long from "long";
import { isUndefined } from "lodash";
import { Link } from "react-router-dom";

import {
  Button,
  ColumnsConfig,
  DownloadFile,
  DownloadFileRef,
  Table,
  Text,
  TextTypes,
  Tooltip,
} from "src/components";
import HeaderSection from "src/views/shared/components/headerSection";
import { AdminUIState } from "src/redux/state";
import { getStatementDiagnostics } from "src/util/api";
import { trustIcon } from "src/util/trust";
import DownloadIcon from "!!raw-loader!assets/download.svg";
import {
  selectStatementByFingerprint,
  selectStatementDiagnosticsReports,
} from "src/redux/statements/statementsSelectors";
import {
  invalidateStatementDiagnosticsRequests,
  refreshStatementDiagnosticsRequests,
  refreshStatements,
} from "src/redux/apiReducers";
import { DiagnosticStatusBadge } from "src/views/statements/diagnostics/diagnosticStatusBadge";
import "./statementDiagnosticsHistoryView.styl";
import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;
import StatementDiagnosticsRequest = cockroach.server.serverpb.StatementDiagnosticsRequest;
import {
  getDiagnosticsStatus,
  sortByCompletedField,
  sortByRequestedAtField,
  sortByStatementFingerprintField,
} from "src/views/statements/diagnostics";
import { trackDownloadDiagnosticsBundle } from "src/util/analytics";
import { shortStatement } from "src/views/statements/statementsTable";
import { summarize } from "src/util/sql/summarize";

type StatementDiagnosticsHistoryViewProps = MapStateToProps & MapDispatchToProps;

const StatementColumn: React.FC<{ fingerprint: string }> = ({ fingerprint }) => {
  const summary = summarize(fingerprint);
  const shortenedStatement = shortStatement(summary, fingerprint);
  const showTooltip = fingerprint !== shortenedStatement;

  if (showTooltip) {
    return (
      <Text textType={TextTypes.Code}>
        <Tooltip
          placement="bottom"
          title={
            <pre className="cl-table-link__description">{ fingerprint }</pre>
          }
          overlayClassName="cl-table-link__statement-tooltip--fixed-width"
        >
          {shortenedStatement}
        </Tooltip>
      </Text>
    );
  }
  return (
    <Text textType={TextTypes.Code}>{shortenedStatement}</Text>
  );
};

class StatementDiagnosticsHistoryView extends React.Component<StatementDiagnosticsHistoryViewProps> {
  columns: ColumnsConfig<IStatementDiagnosticsReport> = [
    {
      key: "activatedOn",
      title: "Activated on",
      sorter: sortByRequestedAtField,
      defaultSortOrder: "descend",
      width: "240px",
      render: (_text, record) => {
        const timestamp = record.requested_at.seconds.toNumber() * 1000;
        return moment(timestamp).format("LL[ at ]h:mm a");
      },
    },
    {
      key: "statement",
      title: "statement",
      sorter: sortByStatementFingerprintField,
      render: (_text, record) => {
        const { getStatementByFingerprint } = this.props;
        const fingerprint = record.statement_fingerprint;
        const statement = getStatementByFingerprint(fingerprint);
        const { implicit_txn: implicitTxn = "true", query } = statement?.key?.key_data || {};

        if (isUndefined(query)) {
          return <StatementColumn fingerprint={fingerprint} />;
        }

        return (
          <Link
            to={ `/statement/${implicitTxn}/${encodeURIComponent(query)}` }
            className="crl-statements-diagnostics-view__statements-link"
          >
            <StatementColumn fingerprint={fingerprint} />
          </Link>
        );
      },
    },
    {
      key: "status",
      title: "status",
      sorter: sortByCompletedField,
      width: "160px",
      render: (_text, record) => (
        <Text>
          <DiagnosticStatusBadge
            status={getDiagnosticsStatus(record)}
          />
        </Text>
      ),
    },
    {
      key: "actions",
      title: "",
      sorter: false,
      width: "160px",
      render: (_text, record) => {
        if (record.completed) {
          return (
            <div className="crl-statements-diagnostics-view__actions-column cell--show-on-hover nodes-table__link">
              <a href={`_admin/v1/stmtbundle/${record.statement_diagnostics_id}`}
                 onClick={() => trackDownloadDiagnosticsBundle(record.statement_fingerprint)}>
                <Button
                  size="small"
                  type="flat"
                  iconPosition="left"
                  icon={() => (
                    <span
                      className="crl-statements-diagnostics-view__icon"
                      dangerouslySetInnerHTML={ trustIcon(DownloadIcon) }
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
    props.refresh();
  }

  renderTableTitle = () => {
    const { diagnosticsReports } = this.props;
    const totalCount = diagnosticsReports.length;

    if (totalCount === 0) {
      return null;
    }

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
  }

  getStatementDiagnostics = async (diagnosticsId: Long) => {
    const request = new StatementDiagnosticsRequest({ statement_diagnostics_id: diagnosticsId });
    const response = await getStatementDiagnostics(request);
    const trace = response.diagnostics?.trace;
    this.downloadRef.current?.download("statement-diagnostics.json", "application/json", trace);
  }

  render() {
    const { diagnosticsReports } = this.props;
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
        { this.renderTableTitle() }
        <div className="diagnostics-history-view__table-container">
          <Table
            pageSize={this.tablePageSize}
            dataSource={dataSource}
            columns={this.columns}
          />
        </div>
        <DownloadFile ref={this.downloadRef}/>
      </section>
    );
  }
}

interface MapStateToProps {
  diagnosticsReports: IStatementDiagnosticsReport[];
  getStatementByFingerprint: (fingerprint: string) => ReturnType<typeof selectStatementByFingerprint>;
}

interface MapDispatchToProps {
  refresh: () => void;
}

const mapStateToProps = (state: AdminUIState): MapStateToProps => ({
  diagnosticsReports: selectStatementDiagnosticsReports(state) || [],
  getStatementByFingerprint: (fingerprint: string) => selectStatementByFingerprint(state, fingerprint),
});

const mapDispatchToProps = (dispatch: Dispatch<Action, AdminUIState>): MapDispatchToProps => ({
  refresh: () => {
    dispatch(invalidateStatementDiagnosticsRequests());
    dispatch(refreshStatementDiagnosticsRequests());
    dispatch(refreshStatements());
  },
});

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  StatementDiagnosticsHistoryViewProps
  >(mapStateToProps, mapDispatchToProps)(StatementDiagnosticsHistoryView);
