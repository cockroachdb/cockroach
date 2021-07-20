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
import { SessionTableTitle } from "./sessionsTableContent";
import { TimestampToMoment } from "src/util/convert";
import { BytesWithPrecision, DATE_FORMAT } from "src/util/format";
import { Link } from "react-router-dom";
import React from "react";

import { Moment } from "moment";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
type ISession = cockroach.server.serverpb.Session;

import { TerminateSessionModalRef } from "./terminateSessionModal";
import { TerminateQueryModalRef } from "./terminateQueryModal";

import { StatementLink } from "src/statementsTable/statementsTableContent";
import { ColumnDescriptor, SortedTable } from "src/sortedtable/sortedtable";

import { Icon } from "antd";
import {
  Dropdown,
  DropdownOption as DropdownItem,
} from "src/dropdown/dropdown";
import { Button } from "src/button/button";
import { Tooltip } from "@cockroachlabs/ui-components";

const cx = classNames.bind(styles);

export interface SessionInfo {
  session: ISession;
}

export class SessionsSortedTable extends SortedTable<SessionInfo> {}

export function byteArrayToUuid(array: Uint8Array) {
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
  const start = TimestampToMoment(session.start);
  const sessionID = byteArrayToUuid(session.id);

  return (
    <div className={cx("sessionLink")}>
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<>Session started at {start.format(DATE_FORMAT)}</>}
      >
        <Link onClick={onClick} to={`${base}/${encodeURIComponent(sessionID)}`}>
          <div>{start.fromNow(true)}</div>
        </Link>
      </Tooltip>
    </div>
  );
};

const AgeLabel = (props: { start: Moment; thingName: string }) => {
  return (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={
        <>
          {props.thingName} started at {props.start.format(DATE_FORMAT)}
        </>
      }
    >
      {props.start.fromNow(true)}
    </Tooltip>
  );
};

export function makeSessionsColumns(
  terminateSessionRef?: React.RefObject<TerminateSessionModalRef>,
  terminateQueryRef?: React.RefObject<TerminateQueryModalRef>,
  onSessionClick?: () => void,
  onTerminateSessionClick?: () => void,
  onTerminateStatementClick?: () => void,
): ColumnDescriptor<SessionInfo>[] {
  return [
    {
      name: "sessionAge",
      title: SessionTableTitle.sessionAge,
      className: cx("cl-table__col-session-age"),
      cell: session =>
        SessionLink({ session: session.session, onClick: onSessionClick }),
      sort: session => TimestampToMoment(session.session.start).valueOf(),
    },
    {
      name: "txnAge",
      title: SessionTableTitle.txnAge,
      className: cx("cl-table__col-session-start"),
      cell: function(session: SessionInfo) {
        if (session.session.active_txn) {
          return AgeLabel({
            start: TimestampToMoment(session.session.active_txn.start),
            thingName: "Transaction",
          });
        }
        return "N/A";
      },
      sort: session => session.session.active_txn?.start.seconds || 0,
    },
    {
      name: "statementAge",
      title: SessionTableTitle.statementAge,
      className: cx("cl-table__col-session-start"),
      cell: function(session: SessionInfo) {
        if (session.session.active_queries?.length > 0) {
          return AgeLabel({
            start: TimestampToMoment(session.session.active_queries[0].start),
            thingName: "Statement",
          });
        }
        return "N/A";
      },
      sort: function(session: SessionInfo): number {
        if (session.session.active_queries?.length > 0) {
          return session.session.active_queries[0].start.seconds.toNumber();
        }
        return 0;
      },
    },
    {
      name: "memUsage",
      title: SessionTableTitle.memUsage,
      className: cx("cl-table__col-session-mem-usage"),
      cell: session =>
        BytesWithPrecision(session.session.alloc_bytes?.toNumber(), 0) +
        "/" +
        BytesWithPrecision(session.session.max_alloc_bytes?.toNumber(), 0),
      sort: session => session.session.alloc_bytes?.toNumber(),
    },
    {
      name: "statement",
      title: SessionTableTitle.statement,
      className: cx("cl-table__col-query-text"),
      cell: session => {
        if (!(session.session.active_queries?.length > 0)) {
          return "N/A";
        }
        const stmt = session.session.active_queries[0].sql;
        const anonStmt = session.session.active_queries[0].sql_anon;
        return (
          <StatementLink
            statement={stmt}
            anonStatement={anonStmt}
            implicitTxn={session.session.active_txn?.implicit}
            search={""}
            app={""}
          />
        );
      },
    },
    {
      name: "actions",
      title: SessionTableTitle.actions,
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
              <Icon type="ellipsis" />
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
    },
  ];
}
