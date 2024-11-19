// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames";
import * as React from "react";
import CopyToClipboard from "react-copy-to-clipboard";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { AdminUIState } from "src/redux/state";

import "./redux.styl";
import { BackToAdvanceDebug } from "../util";

interface ReduxDebugProps extends RouteComponentProps {
  state: AdminUIState;
}

interface ReduxDebugState {
  copied: boolean;
}

export class ReduxDebug extends React.Component<
  ReduxDebugProps,
  ReduxDebugState
> {
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
        <Helmet title="Redux State | Debug" />
        <BackToAdvanceDebug history={this.props.history} />
        <section className="section">
          <h1 className="base-heading">Redux State</h1>
        </section>
        <section className="section">
          <CopyToClipboard
            text={text}
            onCopy={() => this.setState({ copied: true })}
          >
            <span className={spanClass}>
              {this.state.copied ? "Copied." : "Copy to Clipboard"}
            </span>
          </CopyToClipboard>
          <pre className="state-json-box">{text}</pre>
        </section>
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return { state };
}

export default withRouter(connect(mapStateToProps)(ReduxDebug));
