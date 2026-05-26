// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

declare module "assets/*" {
  const _: string;
  export default _;
}

declare module "!!raw-loader!*" {
  const _: string;
  export default _;
}

declare module "!!url-loader!*" {
  const _: string;
  export default _;
}
