// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import hljs from "highlight.js/lib/core";
import sqlLangSyntax from "highlight.js/lib/languages/pgsql";
import React, { useRef, useEffect, memo } from "react";

import { SqlBoxProps } from "./box";
import styles from "./sqlhighlight.module.scss";

const cx = classNames.bind(styles);

hljs.registerLanguage("sql", sqlLangSyntax);
hljs.configure({
  tabReplace: "  ",
});

function HighlightInternal({ value, zone }: SqlBoxProps): React.ReactElement {
  const preNode = useRef<HTMLSpanElement>(null);

  // Apply syntax highlighting when the value changes
  useEffect(() => {
    if (preNode.current) {
      hljs.highlightBlock(preNode.current);
    }
  }, [value]);

  const renderZone = (): React.ReactElement => {
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

  return (
    <>
      <span className={cx("sql-highlight")} ref={preNode}>
        {value}
      </span>
      {zone && (
        <>
          <div className={cx("highlight-divider")} />
          {renderZone()}
        </>
      )}
    </>
  );
}

// Wrap with memo to replicate shouldComponentUpdate behavior:
// only re-render when value changes
export const Highlight = memo(HighlightInternal, (prevProps, nextProps) => {
  return prevProps.value === nextProps.value;
});
