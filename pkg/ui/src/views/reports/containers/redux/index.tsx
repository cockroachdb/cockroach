// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import classNames from "classnames";
import CopyToClipboard from "react-copy-to-clipboard";

import { AdminUIState } from "src/redux/state";

import "./redux.styl";

interface ReduxDebugProps {
  state: AdminUIState;
}

interface ReduxDebugState {
  copied: boolean;
}

class ReduxDebug extends React.Component<ReduxDebugProps, ReduxDebugState> {
  constructor(props: any) {
    super(props);
    this.state = { copied: false };
  }

  render() {
    const text = JSON.stringify(this.props.state, null, 2);
    const spanClass = classNames({
      "copy-to-clipboard-span": true,
      "copy-to-clipboard-span--copied": this.state.copied,
    });

    return (
      <div>
        <Helmet>
          <title>Redux State | Debug</title>
        </Helmet>
        <section className="section"><h1>Redux State</h1></section>
        <section className="section">
          <CopyToClipboard text={ text } onCopy={() => this.setState({ copied: true})}>
            <span className={spanClass}>
              { this.state.copied ? "Copied." : "Copy to Clipboard" }
            </span>
          </CopyToClipboard>
          <pre className="state-json-box">
            { text }
          </pre>
        </section>
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return { state };
}

export default connect(mapStateToProps, null)(ReduxDebug);
