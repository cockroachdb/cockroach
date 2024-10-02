// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

const proxy = require('http-proxy-middleware');

const target = process.env.TARGET;

module.exports = function expressMiddleware(router) {
  if (target) {
    router.use('/_status', proxy.createProxyMiddleware({
      target,
      changeOrigin: true
    }))
  }
}
