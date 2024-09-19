// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

const testsContext = require.context("./src", true, /\.spec\.(ts|tsx)$/);
const cclTestsContext = require.context("./ccl/src", true, /\.spec\.(ts|tsx)$/);

testsContext.keys().forEach(testsContext);
cclTestsContext.keys().forEach(cclTestsContext);
