// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// TODO(mrtracy): Convert the JSON files into JS files to have them obtain types
// directly.
declare module "*.json" {
  const value: any;
  export default value;
}
