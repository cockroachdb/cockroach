// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import hljs from "highlight.js/lib/core";
import sqlLangSyntax from "highlight.js/lib/languages/pgsql";
import React from "react";
import classNames from "classnames/bind";
import styles from "./sqlhighlight.module.scss";
import { SqlBoxProps } from "./box";

const cx = classNames.bind(styles);

hljs.registerLanguage("sql", sqlLangSyntax);
hljs.configure({
  tabReplace: "  ",
});

export class Highlight extends React.Component<SqlBoxProps> {
  preNode: React.RefObject<HTMLPreElement> = React.createRef();

  shouldComponentUpdate(newProps: SqlBoxProps) {
    return newProps.value !== this.props.value;
  }

  componentDidMount() {
    hljs.highlightBlock(this.preNode.current);
  }

  componentDidUpdate() {
    hljs.highlightBlock(this.preNode.current);
  }

  renderZone = () => {
    const { zone } = this.props;
    const zoneConfig = zone.zone_config;
    return (
      <span className={cx("sql-highlight", "hljs")}>
        <span className="hljs-keyword">CONFIGURE ZONE USING</span>
        <br />
        <span className="hljs-label">range_min_bytes = </span>
        <span className="hljs-built_in">{`${String(
          zoneConfig.range_min_bytes,
        )},`}</span>
        <br />
        <span className="hljs-label">range_max_bytes = </span>
        <span className="hljs-built_in">{`${String(
          zoneConfig.range_max_bytes,
        )},`}</span>
        <br />
        <span className="hljs-label">gc.ttlseconds = </span>
        <span className="hljs-built_in">{`${zoneConfig.gc.ttl_seconds},`}</span>
        <br />
        <span className="hljs-label">num_replicas = </span>
        <span className="hljs-built_in">{`${zoneConfig.num_replicas},`}</span>
        <br />
        <span className="hljs-label">constraints = [&apos;</span>
        <span className="hljs-built_in">{String(zoneConfig.constraints)}</span>
        &apos;],
        <br />
        <span className="hljs-label">lease_preferences = [[&apos;</span>
        <span className="hljs-built_in">
          {String(zoneConfig.lease_preferences)}
        </span>
        &apos;]]
      </span>
    );
  };

  render() {
    const { value, zone } = this.props;
    return (
      <>
        <span className={cx("sql-highlight")} ref={this.preNode}>
          {value}
        </span>
        {zone && (
          <>
            <div className={cx("higlight-divider")} />
            {this.renderZone()}
          </>
        )}
      </>
    );
  }
}
