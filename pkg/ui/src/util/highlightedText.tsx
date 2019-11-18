// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

export default function getHighlightedText(text: string, highlight: string) {
  if (highlight.length === 0) {
    return text;
  }
  highlight = highlight.replace(/[°§%()\[\]{}\\?´`'#|;:+-]+/g, "highlightNotDefined");
  const search = highlight.split(" ").map(val => val.toLowerCase()).join("|");
  const parts = text.split(new RegExp(`(${search})`, "gi"));
  return parts.map((part, i) => {
    if (search.includes(part.toLowerCase())) {
      return (
        <span key={i} className="_text-bold">
          {`${part}`}
        </span>
      );
    } else {
      return `${part}`;
    }
  });
}
