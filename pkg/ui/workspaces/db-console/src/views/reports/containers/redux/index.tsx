// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import React, { useState } from "react";
import CopyToClipboard from "react-copy-to-clipboard";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { AdminUIState } from "src/redux/state";

import "./redux.scss";
import { BackToAdvanceDebug } from "../util";

interface ReduxDebugProps extends RouteComponentProps {
  state: AdminUIState;
}

export function ReduxDebug({
  state,
  history,
}: ReduxDebugProps): React.ReactElement {
  const [copied, setCopied] = useState(false);

  const text = JSON.stringify(state, null, 2);
  const spanClass = classNames({
    "copy-to-clipboard-span": true,
    "copy-to-clipboard-span--copied": copied,
  });

  return (
    <div>
      <Helmet title="Redux State | Debug" />
      <BackToAdvanceDebug history={history} />
      <section className="section">
        <h1 className="base-heading">Redux State</h1>
      </section>
      <section className="section">
        <CopyToClipboard text={text} onCopy={() => setCopied(true)}>
          <span className={spanClass}>
            {copied ? "Copied." : "Copy to Clipboard"}
          </span>
        </CopyToClipboard>
        <pre className="state-json-box">{text}</pre>
      </section>
    </div>
  );
}

function mapStateToProps(state: AdminUIState) {
  return { state };
}

export default withRouter(connect(mapStateToProps)(ReduxDebug));
