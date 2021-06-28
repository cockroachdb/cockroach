// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as hljs from "highlight.js";
import React from "react";
import classNames from "classnames/bind";
import styles from "./sqlhighlight.module.styl";
import { SqlBoxProps } from "./box";

const cx = classNames.bind(styles);

export class Highlight extends React.Component<SqlBoxProps> {
  preNode: React.RefObject<HTMLPreElement> = React.createRef();
  preNodeSecondary: React.RefObject<HTMLPreElement> = React.createRef();

  shouldComponentUpdate(newProps: SqlBoxProps) {
    return newProps.value !== this.props.value;
  }

  componentDidMount() {
    hljs.configure({
      tabReplace: "  ",
      languages: ["sql"],
    });
    hljs.highlightBlock(this.preNode.current);
    if (this.preNodeSecondary.current) {
      hljs.highlightBlock(this.preNodeSecondary.current);
    }
  }

  componentDidUpdate() {
    hljs.highlightBlock(this.preNode.current);
    if (this.preNodeSecondary.current) {
      hljs.highlightBlock(this.preNodeSecondary.current);
    }
  }

  render() {
    const { value, secondaryValue } = this.props;
    return (
      <>
        <span className={cx("sql-highlight")} ref={this.preNode}>
          {value}
        </span>
        {secondaryValue && (
          <>
            <div className={cx("higlight-divider")} />
            <span className={cx("sql-highlight")} ref={this.preNodeSecondary}>
              {secondaryValue}
            </span>
          </>
        )}
      </>
    );
  }
}
