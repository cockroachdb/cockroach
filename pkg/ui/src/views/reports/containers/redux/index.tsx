// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
