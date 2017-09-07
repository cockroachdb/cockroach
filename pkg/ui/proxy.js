#!/usr/bin/env node

// Simple proxy server that serves the UI from source and proxies requests to
// another Cockroach instance.

const path = require("path");
const webpack = require("webpack");
const webpackConfig = require("./webpack.app");
const WebpackDevServer = require("webpack-dev-server");

const argv = require("yargs")
  .usage("Usage: $0 <remote-cockroach-ui-url> [options]")
  .demand(1)
  .default("port", 3000, "The port to run this proxy server on")
  .example(`$0 https://myroach:8080`, "Serve UI resources (HTML, JS, CSS) from localhost:8080, " +
    "while serving API data requests from https://myroach:8080")
  .help("h")
  .alias("h", "help")
  .alias("p", "port")
  .argv;

const port = argv.port;
const remote = argv._[0];

console.log(`Booting server at http://localhost:${port}`);

const compiler = webpack(Object.assign(webpackConfig, {
  devtool: "source-map",
}));

const server = new WebpackDevServer(compiler, {
  contentBase: path.join(__dirname, "dist"),
  stats: webpackConfig.stats,
  proxy: [{
    context: ["/_admin", "/_status", "/ts"],
    secure: false,
    target: remote,
  }],
  port: port,
});

server.listen(port, "127.0.0.1");
