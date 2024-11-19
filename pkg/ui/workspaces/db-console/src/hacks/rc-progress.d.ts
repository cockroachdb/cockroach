// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
  export class Line extends React.Component<LineProps, {}> {}
}
