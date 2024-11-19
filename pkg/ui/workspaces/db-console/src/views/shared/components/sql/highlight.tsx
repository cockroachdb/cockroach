// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import * as hljs from "highlight.js";
import React from "react";

import { SqlBoxProps } from "./box";
import styles from "./sqlhighlight.module.styl";

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
            <div className={cx("highlight-divider")} />
            <span className={cx("sql-highlight")} ref={this.preNodeSecondary}>
              {secondaryValue}
            </span>
          </>
        )}
      </>
    );
  }
}
