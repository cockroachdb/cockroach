#!/usr/bin/env node

// Simple proxy server that serves the UI from one Cockroach instance and
// proxies requests to another Cockroach instance.

const express = require('express');
const proxy = require('http-proxy-middleware');
const webpackDevMiddleware = require('webpack-dev-middleware');
const webpack = require('webpack');
const webpackConfig = require('./webpack.config');

const app = express();

const compiler = webpack(Object.assign(webpackConfig, {
  devtool: 'source-map',
}));

const argv = require('yargs')
  .usage('Usage: $0 <remote-cockroach-ui-url> [options]')
  .demand(1)
  .default('port', 3000, 'The port to run this proxy server on')
  .example(`$0 https://myroach:8080`, 'Serve UI resources (HTML, JS, CSS) from localhost:8080, while serving API data requests from https://myroach:8080')
  .help('h')
  .alias('h', 'help')
  .alias('p', 'port')
  .argv;

const port = argv.port, remote = argv._[0];

app.use(webpackDevMiddleware(compiler, {}));

app.use('/', proxy({
  target: remote,
  secure: false,
}));

console.log(`Proxying requests to ${remote} at http://localhost:${port}`);

app.listen(port);
