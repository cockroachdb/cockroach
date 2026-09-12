// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

import "./consoleHeaderBrand.scss";

// ConsoleHeaderBrand renders the left side of the 2026 black masthead: the
// white symbol-only Cockroach mark followed by a "/ CockroachDB" breadcrumb,
// matching the cc-console / migration-assistant top-nav treatment. The mark
// draws with `currentColor`, so it inherits the header's white text color and
// stays legible on the dark bar.
export function ConsoleHeaderBrand(): React.ReactElement {
  return (
    <div className="console-header-brand">
      <svg
        className="console-header-brand__mark"
        width="23"
        height="24"
        viewBox="0 0 161.31 166.68"
        fill="currentColor"
        xmlns="http://www.w3.org/2000/svg"
        aria-label="Cockroach Labs"
      >
        <path d="M139.81,16.13V5.38c0-2.97-2.41-5.38-5.38-5.38h-37.64c-2.97,0-5.38,2.41-5.38,5.38v5.38c0,5.94-4.81,10.75-10.75,10.75h0c-5.94,0-10.75-4.81-10.75-10.75v-5.38c0-2.97-2.41-5.38-5.38-5.38H26.89c-2.97,0-5.38,2.41-5.38,5.38v10.75c0,2.97-2.41,5.38-5.38,5.38H5.38c-2.97,0-5.38,2.41-5.38,5.38v48.39c0,2.97,2.41,5.38,5.38,5.38h10.75c2.97,0,5.38-2.41,5.38-5.38V26.89c0-2.97,2.41-5.38,5.38-5.38h32.26c5.94,0,10.75,4.81,10.75,10.75h0c0,5.94-4.81,10.75-10.75,10.75h-5.38c-2.97,0-5.38,2.41-5.38,5.38v91.4c0,2.97,2.41,5.38,5.38,5.38h10.75c2.97,0,5.38,2.41,5.38,5.38v10.75c0,2.97,2.41,5.38,5.38,5.38h10.75c2.97,0,5.38-2.41,5.38-5.38v-10.75c0-2.97,2.41-5.38,5.38-5.38h10.75c2.97,0,5.38-2.41,5.38-5.38V48.39c0-2.97-2.41-5.38-5.38-5.38h-5.38c-5.94,0-10.75-4.81-10.75-10.75h0c0-5.94,4.81-10.75,10.75-10.75h32.26c2.97,0,5.38,2.41,5.38,5.38v48.39c0,2.97,2.41,5.38,5.38,5.38h10.75c2.97,0,5.38-2.41,5.38-5.38V26.89c0-2.97-2.41-5.38-5.38-5.38h-10.75c-2.97,0-5.38-2.41-5.38-5.38h.01ZM91.42,53.76v80.65c0,5.94-4.81,10.75-10.75,10.75h0c-5.94,0-10.75-4.81-10.75-10.75V53.76c0-5.94,4.81-10.75,10.75-10.75h0c5.94,0,10.75,4.81,10.75,10.75Z" />
      </svg>
      <span className="console-header-brand__separator">/</span>
      <span className="console-header-brand__label">CockroachDB</span>
    </div>
  );
}
