// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

"use strict";

const path = require("path");
const rimraf = require("rimraf");
const webpack = require("webpack");
const CopyWebpackPlugin = require("copy-webpack-plugin");
const VisualizerPlugin = require("webpack-visualizer-plugin");
const StringReplacePlugin = require("string-replace-webpack-plugin");
const WebpackBar = require("webpackbar");

// Remove a broken dependency that Yarn insists upon installing before every
// Webpack compile. We also do this when installing dependencies via Make, but
// it"s common to run e.g. `yarn add` manually without re-running Make, which
// will reinstall the broken dependency. The error this dependency causes is
// horribly cryptic, so it"s important to remove it aggressively.
//
// See: https://github.com/yarnpkg/yarn/issues/2987
class RemoveBrokenDependenciesPlugin {
  apply(compiler) {
    compiler.plugin("compile", () => rimraf.sync("./node_modules/@types/node"));
  }
}

const proxyPrefixes = ["/_admin", "/_status", "/ts", "/login", "/logout"];
function shouldProxy(reqPath) {
  if (reqPath === "/") {
    return true;
  }
  return proxyPrefixes.some((prefix) => reqPath.startsWith(prefix));
}

// tslint:disable:object-literal-sort-keys
module.exports = (env, argv) => {
  let localRoots = [path.resolve(__dirname)];
  if (env.dist === "ccl") {
    // CCL modules shadow OSS modules.
    localRoots.unshift(path.resolve(__dirname, "ccl"));
  }

  const config = {
    entry: ["./src/index.tsx"],
    output: {
      filename: "bundle.js",
      path: path.resolve(__dirname, `dist${env.dist}`),
    },

    mode: argv.mode || "production",

    resolve: {
      // Add resolvable extensions.
      extensions: [".ts", ".tsx", ".js", ".json", ".styl", ".css"],
      // First check for local modules, then for third-party modules from
      // node_modules.
      //
      // These module roots are transformed into absolute paths, by
      // path.resolve, to ensure that only the exact directory is checked.
      // Relative paths would trigger the resolution behavior used by Node.js
      // for "node_modules", i.e., checking for a "node_modules" directory in
      // the current directory *or any parent directory*.
      modules: [...localRoots, path.resolve(__dirname, "node_modules")],
      alias: { oss: path.resolve(__dirname) },
    },

    module: {
      rules: [
        { test: /\.css$/, use: ["style-loader", "css-loader"] },
        {
          test: /\.module\.styl$/,
          use: [
            "cache-loader",
            "style-loader",
            {
              loader: "css-loader",
              options: {
                modules: {
                  localIdentName: "[local]--[hash:base64:5]",
                },
                importLoaders: 1,
              },
            },
            {
              loader: "stylus-loader",
              options: {
                use: [require("nib")()],
              },
            },
          ],
        },
        {
          test: /(?<!\.module)\.styl$/,
          use: [
            "cache-loader",
            "style-loader",
            "css-loader",
            {
              loader: "stylus-loader",
              options: {
                use: [require("nib")()],
              },
            },
          ],
        },
        {
          test: /\.(png|jpg|gif|svg|eot|ttf|woff|woff2)$/,
          loader: "url-loader",
          options: {
            limit: 10000,
          },
        },
        { test: /\.html$/, loader: "file-loader" },
        {
          test: /\.js$/,
          include: localRoots,
          use: ["cache-loader", "babel-loader"],
        },
        {
          test: /\.(ts|tsx)?$/,
          include: localRoots,
          exclude: /\/node_modules/,
          use: [
            "cache-loader",
            "babel-loader",
            { loader: "ts-loader", options: { happyPackMode: true } },
          ],
        },

        // All output ".js" files will have any sourcemaps re-processed by "source-map-loader".
        {
          enforce: "pre",
          test: /\.js$/,
          loader: "source-map-loader",
          include: localRoots,
          exclude: /\/node_modules/,
        },
      ],
    },

    plugins: [
      new RemoveBrokenDependenciesPlugin(),
      // See "DLLs for speedy builds" in the README for details.
      new webpack.DllReferencePlugin({
        context: path.resolve(__dirname, `dist${env.dist}`),
        manifest: require(`./protos.${env.dist}.manifest.json`),
      }),
      new webpack.DllReferencePlugin({
        context: path.resolve(__dirname, `dist${env.dist}`),
        manifest: require("./vendor.oss.manifest.json"),
      }),
      new CopyWebpackPlugin([{ from: "favicon.ico", to: "favicon.ico" }]),
      new VisualizerPlugin({ filename: `../dist/stats.${env.dist}.html` }),
      // use WebpackBar instead of webpack dashboard to fit multiple webpack dev server outputs (db-console and cluster-ui)
      new WebpackBar({
        name: "db-console",
        color: "orange",
        profile: true,
      }),
    ],

    stats: "errors-only",

    devServer: {
      contentBase: path.join(__dirname, `dist${env.dist}`),
      index: "",
      proxy: {
        "/": {
          secure: false,
          target: process.env.TARGET,
        },
      },
    },

    watchOptions: {
      poll: 1000,
      ignored: /node_modules/,
    },

    // Max size of is set to 4Mb to disable warning message and control
    // the growing size of bundle over time.
    performance: {
      maxEntrypointSize: 4000000,
      maxAssetSize: 4000000,
    },
  };

  // The purpose of this is to have zero impact on production code and at the
  // same time to be able inject testing code with SIMULATE_METRICS_LOAD env variable.
  // It injects Redux middleware which generates metrics datapoints for performance
  // testing.
  if (
    process.env.SIMULATE_METRICS_LOAD === "true" &&
    argv.mode !== "production"
  ) {
    config.module.rules.push({
      test: /src\/redux\/state.ts$/,
      loader: StringReplacePlugin.replace({
        replacements: [
          {
            pattern: /import rootSaga from ".\/sagas";/gi, // match last 'import' expression in module.
            replacement: function (match, p) {
              return `${match}\nimport { fakeMetricsDataGenerationMiddleware } from "src/test-utils/fakeMetricsDataGenerationMiddleware";`;
            },
          },
          {
            pattern: /(?<=applyMiddleware\((.*))\),$/gm,
            replacement: function (match) {
              return `, fakeMetricsDataGenerationMiddleware${match}`;
            },
          },
        ],
      }),
    });
    config.plugins.push(new StringReplacePlugin());
  }
  return config;
};
