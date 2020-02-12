// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(benesch): upstream this.

declare module "rc-progress" {
    export interface LineProps {
      strokeColor?: string;
      strokeWidth?: number;
      trailWidth?: number;
      trailColor?: string;
      className?: string;
      percent?: number;
    }
    export class Line extends React.Component<LineProps, {}> {
    }
  }
  