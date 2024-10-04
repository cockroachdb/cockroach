// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// TODO(mrtracy): Convert the JSON files into JS files to have them obtain types
// directly.
declare module "*.json" {
  const value: any;
  export default value;
}
