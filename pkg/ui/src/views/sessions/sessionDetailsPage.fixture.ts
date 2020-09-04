// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {refreshSessions} from "src/redux/apiReducers";
import {createMemoryHistory} from "history";
import {SessionDetailsProps} from "src/views/sessions/sessionDetails";
import {activeSession, idleSession, idleTransactionSession} from "src/views/sessions/sessionsPage.fixture";
import {sessionAttr} from "src/util/constants";

const history = createMemoryHistory({ initialEntries: ["/sessions"]});

const sessionDetailsPropsBase: SessionDetailsProps = {
  id: "blah",
  nodeNames: {
    1: "localhost",
  },
  sessionError: null,
  session: null,
  history,
  location: {
    "pathname": "/sessions/blah",
    "search": "",
    "hash": "",
    "state": null,
  },
  "match": {
    "path": "/sessions/blah",
    "url": "/sessions/blah",
    "isExact": true,
    "params": {[sessionAttr]: "blah"},
  },
  refreshSessions: (() => {}) as (typeof refreshSessions),
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

export const sessionDetailsNotFound: SessionDetailsProps = {
  ...sessionDetailsPropsBase,
  session: {"session": null},
};
