import * as React from "react";
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
  static title() {
    return "Redux State";
  }

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
