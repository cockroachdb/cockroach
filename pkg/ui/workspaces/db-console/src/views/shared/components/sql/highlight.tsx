// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import * as hljs from "highlight.js";
import React, { memo, useEffect, useRef } from "react";

import { SqlBoxProps } from "./box";
import styles from "./sqlhighlight.module.scss";

const cx = classNames.bind(styles);

// Configure hljs once at module level.
hljs.configure({
  tabReplace: "  ",
  languages: ["sql"],
});

function HighlightInternal({
  value,
  secondaryValue,
}: SqlBoxProps): React.ReactElement {
  const preNode = useRef<HTMLPreElement>(null);
  const preNodeSecondary = useRef<HTMLPreElement>(null);

  // Highlight on mount and whenever value/secondaryValue changes.
  useEffect(() => {
    hljs.highlightBlock(preNode.current);
    if (preNodeSecondary.current) {
      hljs.highlightBlock(preNodeSecondary.current);
    }
  }, [value, secondaryValue]);

  return (
    <>
      <span className={cx("sql-highlight")} ref={preNode}>
        {value}
      </span>
      {secondaryValue && (
        <>
          <div className={cx("highlight-divider")} />
          <span className={cx("sql-highlight")} ref={preNodeSecondary}>
            {secondaryValue}
          </span>
        </>
      )}
    </>
  );
}

// Wrap with memo to replicate shouldComponentUpdate behavior:
// only re-render when value changes.
export const Highlight = memo(HighlightInternal, (prevProps, nextProps) => {
  return prevProps.value === nextProps.value;
});
