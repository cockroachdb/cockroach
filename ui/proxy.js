#!/usr/bin/env node

// Simple proxy server that serves the UI from one Cockroach instance and
// proxies requests to another Cockroach instance.

var express = require('express');
var app      = express();
var httpProxy = require('http-proxy');
var apiProxy = httpProxy.createProxyServer({secure:false});

var argv = require('yargs')
  .usage('Usage: $0 <remote-cockroach-ui-port> [options]')
  .demand(1)
  .default('local', 'http://localhost:8080', 'Cockroach instance ui port to serve UI files from')
  .default('port', 3000, 'The port to run this proxy server')
  .example(`$0 https://myroach:8080`, 'Serve the UI from localhost:8080 and proxy requests to myroach:8080')
  .help('h')
  .alias('h', 'help')
  .alias('p', 'port')
  .alias('l', 'local')
  .argv;

var local = argv.local,
  remote = argv._[0],
  port = argv.port;

console.log(`Proxying requests from ${local} to ${remote} at http://localhost:${port}`);

app.all("/_admin/v1*", function(req, res) {
  apiProxy.web(req, res, {target: remote});
});

app.all("/_status*", function(req, res) {
  apiProxy.web(req, res, {target: remote});
});

app.all("/ts/*", function(req, res) {
  apiProxy.web(req, res, {target: remote});
});

app.all("/*", function(req, res) {
  apiProxy.web(req, res, {target: local});
});

app.listen(port);
