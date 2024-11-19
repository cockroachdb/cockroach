// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createMemoryHistory } from "history";

import {
  CancelSessionRequestMessage,
  CancelQueryRequestMessage,
} from "src/api/terminateQueryApi";
import { sessionAttr } from "src/util/constants";

import { SessionDetailsProps } from "./sessionDetails";
import {
  activeSession,
  closedSession,
  idleSession,
  idleTransactionSession,
} from "./sessionsPage.fixture";

const history = createMemoryHistory({ initialEntries: ["/sessions"] });

const sessionDetailsPropsBase: SessionDetailsProps = {
  id: "blah",
  nodeNames: {
    1: "localhost",
  },
  sessionError: null,
  session: null,
  history,
  location: {
    pathname: "/sessions/blah",
    search: "",
    hash: "",
    state: null,
  },
  match: {
    path: "/sessions/blah",
    url: "/sessions/blah",
    isExact: true,
    params: { [sessionAttr]: "blah" },
  },
  setTimeScale: () => {},
  refreshSessions: () => {},
  cancelSession: (_req: CancelSessionRequestMessage) => {},
  cancelQuery: (_req: CancelQueryRequestMessage) => {},
  refreshNodes: () => {},
  refreshNodesLiveness: () => {},
  uiConfig: {
    showGatewayNodeLink: true,
  },
};

export const sessionDetailsIdlePropsFixture: SessionDetailsProps = {
  ...sessionDetailsPropsBase,
  session: idleSession,
};

export const sessionDetailsActiveTxnPropsFixture: SessionDetailsProps = {
  ...sessionDetailsPropsBase,
  session: idleTransactionSession,
};

export const sessionDetailsActiveStmtPropsFixture: SessionDetailsProps = {
  ...sessionDetailsPropsBase,
  session: activeSession,
};

export const sessionDetailsClosedPropsFixture: SessionDetailsProps = {
  ...sessionDetailsPropsBase,
  session: closedSession,
};

export const sessionDetailsNotFound: SessionDetailsProps = {
  ...sessionDetailsPropsBase,
  session: { session: null },
};
