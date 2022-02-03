// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames/bind";

import styles from "./sessionsTable.module.scss";
import { TimestampToMoment } from "src/util/convert";
import { BytesWithPrecision } from "src/util/format";
import { Link } from "react-router-dom";
import React from "react";

import moment from "moment";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
type ISession = cockroach.server.serverpb.Session;

import { TerminateSessionModalRef } from "./terminateSessionModal";
import { TerminateQueryModalRef } from "./terminateQueryModal";
import { ColumnDescriptor, SortedTable } from "src/sortedtable/sortedtable";

import { Icon } from "@cockroachlabs/ui-components";
import { CircleFilled } from "src/icon/circleFilled";

import {
  Dropdown,
  DropdownOption as DropdownItem,
} from "src/dropdown/dropdown";
import { Button } from "src/button/button";
import { Tooltip } from "@cockroachlabs/ui-components";
import { computeOrUseStmtSummary } from "../util";
import { StatementLinkTarget } from "../statementsTable";
import {
  statisticsTableTitles,
  StatisticType,
} from "../statsTableUtil/statsTableUtil";

const cx = classNames.bind(styles);

export interface SessionInfo {
  session: ISession;
}

export class SessionsSortedTable extends SortedTable<SessionInfo> {}

export function byteArrayToUuid(array: Uint8Array): string {
  const hexDigits: string[] = [];
  array.forEach(t => hexDigits.push(t.toString(16).padStart(2, "0")));
  return [
    hexDigits.slice(0, 4).join(""),
    hexDigits.slice(4, 6).join(""),
    hexDigits.slice(6, 8).join(""),
    hexDigits.slice(8, 10).join(""),
    hexDigits.slice(10, 16).join(""),
  ].join("-");
}

const SessionLink = (props: { session: ISession; onClick?: () => void }) => {
  const { session, onClick } = props;

  const base = `/session`;
  const sessionID = byteArrayToUuid(session.id);

  return (
    <div className={cx("sessionLink")}>
      <Link onClick={onClick} to={`${base}/${encodeURIComponent(sessionID)}`}>
        <div>{formatSessionStart(session)}</div>
      </Link>
    </div>
  );
};

const StatementTableCell = (props: { session: ISession }) => {
  const { session } = props;

  if (!(session.active_queries?.length > 0)) {
    if (session.last_active_query == "") {
      return <div>{"N/A"}</div>;
    }
    return <div>{session.last_active_query}</div>;
  }
  const stmt = session.active_queries[0];
  const sql = stmt.sql;
  const stmtSummary = session.active_queries[0].sql_summary;
  const stmtCellText = computeOrUseStmtSummary(sql, stmtSummary);
  return (
    <Link
      to={StatementLinkTarget({
        statementFingerprintID: stmt.id,
        statementNoConstants: stmt.sql_no_constants,
        implicitTxn: session.active_txn?.implicit,
        app: session.application_name,
      })}
    >
      <Tooltip placement="bottom" style="tableTitle" content={<>{sql}</>}>
        <div className={cx("cl-table__col-query-text")}>{stmtCellText}</div>
      </Tooltip>
    </Link>
  );
};

function formatSessionStart(session: ISession): string {
  const formatStr = "MMM DD, YYYY [at] h:mm A";
  const start = moment.unix(Number(session.start.seconds)).utc();

  return start.format(formatStr);
}

function formatStatementStart(session: ISession): string {
  if (session.active_queries.length == 0) {
    return "N/A";
  }
  const formatStr = "MMM DD, YYYY [at] h:mm A";
  const start = moment
    .unix(Number(session.active_queries[0].start.seconds))
    .utc();

  return start.format(formatStr);
}

const SessionStatus = (props: { session: ISession }) => {
  const { session } = props;
  const status = session.active_queries.length > 0 ? "Active" : "Idle";
  const classname =
    session.active_queries.length > 0
      ? "session-status-icon__active"
      : "session-status-icon__idle";
  return (
    <div>
      <CircleFilled className={cx(classname)} />
      <span>{status}</span>
    </div>
  );
};

export function makeSessionsColumns(
  statType: StatisticType,
  terminateSessionRef?: React.RefObject<TerminateSessionModalRef>,
  terminateQueryRef?: React.RefObject<TerminateQueryModalRef>,
  onSessionClick?: () => void,
  onTerminateSessionClick?: () => void,
  onTerminateStatementClick?: () => void,
): ColumnDescriptor<SessionInfo>[] {
  const columns: ColumnDescriptor<SessionInfo>[] = [
    {
      name: "sessionStart",
      title: statisticsTableTitles.sessionStart(statType),
      className: cx("cl-table__col-session"),
      cell: session =>
        SessionLink({ session: session.session, onClick: onSessionClick }),
      sort: session => session.session.start.seconds,
      alwaysShow: true,
    },
    {
      name: "sessionDuration",
      title: statisticsTableTitles.sessionDuration(statType),
      className: cx("cl-table__col-session"),
      cell: session => TimestampToMoment(session.session.start).fromNow(true),
      sort: session => TimestampToMoment(session.session.start).valueOf(),
    },
    {
      name: "status",
      title: statisticsTableTitles.status(statType),
      className: cx("cl-table__col-session"),
      cell: session => SessionStatus(session),
      sort: session => session.session.active_queries.length,
    },
    {
      name: "mostRecentStatement",
      title: statisticsTableTitles.mostRecentStatement(statType),
      className: cx("cl-table__col-query-text"),
      cell: session => StatementTableCell(session),
      sort: session => session.session.last_active_query,
    },
    {
      name: "statementStartTime",
      title: statisticsTableTitles.statementStartTime(statType),
      className: cx("cl-table__col-session"),
      cell: session => formatStatementStart(session.session),
      sort: session =>
        session.session.active_queries.length > 0
          ? session.session.active_queries[0].start.seconds
          : 0,
    },
    {
      name: "memUsage",
      title: statisticsTableTitles.memUsage(statType),
      className: cx("cl-table__col-session"),
      cell: session =>
        BytesWithPrecision(session.session.alloc_bytes?.toNumber(), 0) +
        "/" +
        BytesWithPrecision(session.session.max_alloc_bytes?.toNumber(), 0),
      sort: session => session.session.alloc_bytes?.toNumber(),
    },
    {
      name: "clientAddress",
      title: statisticsTableTitles.clientAddress(statType),
      className: cx("cl-table__col-session"),
      cell: session => session.session.client_address,
      sort: session => session.session.client_address,
    },
    {
      name: "username",
      title: statisticsTableTitles.username(statType),
      className: cx("cl-table__col-session"),
      cell: session => session.session.username,
      sort: session => session.session.username,
    },
    {
      name: "applicationName",
      title: statisticsTableTitles.applicationName(statType),
      className: cx("cl-table__col-session"),
      cell: session => session.session.application_name,
      sort: session => session.session.application_name,
    },
    {
      name: "actions",
      title: statisticsTableTitles.actions(statType),
      className: cx("cl-table__col-session-actions"),
      titleAlign: "right",
      cell: ({ session }) => {
        const menuItems: DropdownItem[] = [
          {
            value: "terminateStatement",
            name: "Terminate Statement",
            disabled: session.active_queries?.length === 0,
          },
          {
            value: "terminateSession",
            name: "Terminate Session",
          },
        ];

        const onMenuItemChange = (
          value: "terminateStatement" | "terminateSession",
        ) => {
          switch (value) {
            case "terminateSession":
              onTerminateSessionClick && onTerminateSessionClick();
              terminateSessionRef?.current?.showModalFor({
                session_id: session.id,
                node_id: session.node_id.toString(),
              });
              break;
            case "terminateStatement":
              if (session.active_queries?.length > 0) {
                onTerminateStatementClick && onTerminateStatementClick();
                terminateQueryRef?.current?.showModalFor({
                  query_id: session.active_queries[0].id,
                  node_id: session.node_id.toString(),
                });
              }
              break;
            default:
              break;
          }
        };

        const renderDropdownToggleButton: JSX.Element = (
          <>
            <Button type="secondary" size="small">
              <Icon
                iconName="Ellipsis"
                className="cluster-row__status-icon"
                color="warning-color"
              />
            </Button>
          </>
        );

        return (
          <Dropdown
            items={menuItems}
            customToggleButton={renderDropdownToggleButton}
            onChange={onMenuItemChange}
            className={cx("session-action--dropdown")}
            menuPosition="right"
          />
        );
      },
      alwaysShow: true,
    },
  ];

  return columns;
}
