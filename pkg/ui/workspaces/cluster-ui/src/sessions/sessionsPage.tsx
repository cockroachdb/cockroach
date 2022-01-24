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
import { isNil } from "lodash";

import { syncHistory } from "src/util/query";
import { appAttr } from "src/util/constants";
import {
  makeSessionsColumns,
  SessionInfo,
  SessionsSortedTable,
} from "./sessionsTable";
import { RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import { sessionsTable } from "src/util/docs";

import emptyTableResultsIcon from "../assets/emptyState/empty-table-results.svg";
import SQLActivityError from "../sqlActivity/errorComponent";

import { Pagination, ResultsPerPageLabel } from "src/pagination";
import {
  SortSetting,
  ISortedTablePagination,
  updateSortSettingQueryParamsOnTab,
} from "src/sortedtable";
import { Loading } from "src/loading";
import { Anchor } from "src/anchor";
import { EmptyTable } from "src/empty";

import TerminateQueryModal, {
  TerminateQueryModalRef,
} from "./terminateQueryModal";
import TerminateSessionModal, {
  TerminateSessionModalRef,
} from "./terminateSessionModal";

import { UIConfigState } from "src/store";
import {
  ICancelSessionRequest,
  ICancelQueryRequest,
} from "src/store/terminateQuery";

import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";
import sessionPageStyles from "./sessionPage.module.scss";

const statementsPageCx = classNames.bind(statementsPageStyles);
const sessionsPageCx = classNames.bind(sessionPageStyles);

export interface OwnProps {
  sessions: SessionInfo[];
  sessionsError: Error | Error[];
  sortSetting: SortSetting;
  refreshSessions: () => void;
  cancelSession: (payload: ICancelSessionRequest) => void;
  cancelQuery: (payload: ICancelQueryRequest) => void;
  uiConfig?: UIConfigState["pages"]["sessions"];
  onPageChanged?: (newPage: number) => void;
  onSortingChange?: (
    name: string,
    columnTitle: string,
    ascending: boolean,
  ) => void;
  onSessionClick?: () => void;
  onTerminateSessionClick?: () => void;
  onTerminateStatementClick?: () => void;
}

export interface SessionsPageState {
  pagination: ISortedTablePagination;
}

export type SessionsPageProps = OwnProps & RouteComponentProps<any>;

export class SessionsPage extends React.Component<
  SessionsPageProps,
  SessionsPageState
> {
  terminateSessionRef: React.RefObject<TerminateSessionModalRef>;
  terminateQueryRef: React.RefObject<TerminateQueryModalRef>;
  static defaultProps = {
    uiConfig: { showTerminateActions: true },
  };

  constructor(props: SessionsPageProps) {
    super(props);
    this.state = {
      pagination: {
        pageSize: 20,
        current: 1,
      },
    };
    this.terminateSessionRef = React.createRef();
    this.terminateQueryRef = React.createRef();

    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const ascending = (searchParams.get("ascending") || undefined) === "true";
    const columnTitle = searchParams.get("columnTitle") || undefined;
    const sortSetting = this.props.sortSetting;

    if (
      this.props.onSortingChange &&
      columnTitle &&
      (sortSetting.columnTitle != columnTitle ||
        sortSetting.ascending != ascending)
    ) {
      this.props.onSortingChange("Sessions", columnTitle, ascending);
    }
  }

  changeSortSetting = (ss: SortSetting): void => {
    if (this.props.onSortingChange) {
      this.props.onSortingChange("Sessions", ss.columnTitle, ss.ascending);
    }

    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      this.props.history,
    );
  };

  resetPagination = (): void => {
    this.setState(prevState => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  };

  componentDidMount(): void {
    this.props.refreshSessions();
  }

  componentDidUpdate = (): void => {
    const { history, sortSetting } = this.props;

    this.props.refreshSessions();
    updateSortSettingQueryParamsOnTab(
      "Sessions",
      sortSetting,
      {
        ascending: false,
        columnTitle: "statementAge",
      },
      history,
    );
  };

  onChangePage = (current: number): void => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
    this.props.onPageChanged(current);
  };

  renderSessions = (): React.ReactElement => {
    const sessionsData = this.props.sessions;
    const { pagination } = this.state;

    return (
      <>
        <section>
          <div className={statementsPageCx("cl-table-statistic")}>
            <h4 className={statementsPageCx("cl-count-title")}>
              <ResultsPerPageLabel
                pagination={{
                  ...pagination,
                  total: this.props.sessions.length,
                }}
                pageName={"active sessions"}
                selectedApp={appAttr}
              />
            </h4>
          </div>
          <SessionsSortedTable
            className="sessions-table"
            data={sessionsData}
            columns={makeSessionsColumns(
              this.terminateSessionRef,
              this.terminateQueryRef,
              this.props.uiConfig?.showTerminateActions,
              this.props.onSessionClick,
              this.props.onTerminateStatementClick,
              this.props.onTerminateSessionClick,
            )}
            renderNoResult={
              <EmptyTable
                title="No sessions are currently running"
                icon={emptyTableResultsIcon}
                message="Sessions show you which statements and transactions are running for the active session."
                footer={
                  <Anchor href={sessionsTable} target="_blank">
                    Learn more about sessions
                  </Anchor>
                }
              />
            }
            sortSetting={this.props.sortSetting}
            onChangeSortSetting={this.changeSortSetting}
            pagination={pagination}
          />
        </section>
        <Pagination
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={sessionsData.length}
          onChange={this.onChangePage}
        />
      </>
    );
  };

  render(): React.ReactElement {
    const { cancelSession, cancelQuery } = this.props;
    return (
      <div className={sessionsPageCx("sessions-page")}>
        <Loading
          loading={isNil(this.props.sessions)}
          error={this.props.sessionsError}
          render={this.renderSessions}
          renderError={() =>
            SQLActivityError({
              statsType: "sessions",
            })
          }
        />
        <TerminateSessionModal
          ref={this.terminateSessionRef}
          cancel={cancelSession}
        />
        <TerminateQueryModal
          ref={this.terminateQueryRef}
          cancel={cancelQuery}
        />
      </div>
    );
  }
}

export default SessionsPage;
