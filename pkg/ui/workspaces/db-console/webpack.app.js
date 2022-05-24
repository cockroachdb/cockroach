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
  return proxyPrefixes.some(prefix => reqPath.startsWith(prefix));
}

// tslint:disable:object-literal-sort-keys
module.exports = (env, argv) => {
  env = env || {};
  const isBazelBuild = !!process.env.BAZEL_TARGET;

  let localRoots = [path.resolve(__dirname)];
  if (env.dist === "ccl") {
    // CCL modules shadow OSS modules.
    localRoots.unshift(path.resolve(__dirname, "ccl"));
  }

  let plugins = [
    new RemoveBrokenDependenciesPlugin(),
    new CopyWebpackPlugin([
      { from: path.resolve(__dirname, "favicon.ico"), to: "favicon.ico" },
      {
        from: path.resolve(
          !isBazelBuild ? __dirname : "",
          !isBazelBuild ? "../.." : "",
          "node_modules/list.js/dist/list.min.js",
        ),
        to: path.resolve(__dirname, "../../dist_vendor/list.min.js"),
      },
    ]),
    // use WebpackBar instead of webpack dashboard to fit multiple webpack dev server outputs (db-console and cluster-ui)
    new WebpackBar({
      name: "db-console",
      color: "orange",
      reporters: [ (env.WEBPACK_WATCH || env.WEBPACK_SERVE) ? "basic" : "fancy" ],
      profile: true,
    }),
  ];

  // First check for local modules, then for third-party modules from
  // node_modules.
  let modules = [
    ...localRoots,
    path.resolve(__dirname, "node_modules"),
    path.resolve(__dirname, "../..", "node_modules"),
  ];

  if (isBazelBuild) {
    // required for bazel build to resolve properly dependencies
    modules.push("node_modules");
  }

  // Exclude DLLPlugin when build with Bazel because bazel handles caching on its own
  if (!isBazelBuild && !env.WEBPACK_WATCH && !env.WEBPACK_SERVE) {
    plugins = plugins.concat([
      // See "DLLs for speedy builds" in the README for details.
      new webpack.DllReferencePlugin({
        context: path.resolve(__dirname, `dist${env.dist}`),
        manifest: require(env.protos_manifest ||
          `./protos.${env.dist}.manifest.json`),
      }),
      new webpack.DllReferencePlugin({
        context: path.resolve(__dirname, `dist${env.dist}`),
        manifest: require(env.vendor_manifest || "./vendor.oss.manifest.json"),
      }),
    ]);
  }

  const config = {
    entry: [path.resolve(__dirname, "./src/index.tsx")],
    output: {
      filename: "bundle.js",
      path: path.resolve(env.output || `../../dist${env.dist}`, "assets"),
    },

    mode: argv.mode || "production",

    resolve: {
      // Add resolvable extensions.
      extensions: [".ts", ".tsx", ".js", ".json", ".styl", ".css"],
      modules: modules,
      alias: {
        oss: path.resolve(__dirname),
        ccl: path.resolve(__dirname, "ccl"),
        "src/js/protos": "@cockroachlabs/crdb-protobuf-client",
        "ccl/src/js/protos": "@cockroachlabs/crdb-protobuf-client-ccl",
      },
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
          exclude: [
            /node_modules/,
            /src\/js/,
            /ccl\/src\/js/,
            /cluster-ui\/dist/,
          ],
          use: ["cache-loader", "babel-loader"],
        },
        {
          test: /\.(ts|tsx)?$/,
          include: localRoots,
          exclude: [
            /node_modules/,
            /src\/js/,
            /ccl\/src\/js/,
            /cluster-ui\/dist/,
          ],
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
          exclude: [
            /node_modules/,
            /src\/js/,
            /ccl\/src\/js/,
            /cluster-ui\/dist/,
          ],
        },
      ],
    },

    plugins: plugins,

    stats: "errors-only",

    devServer: {
      contentBase: path.join(__dirname, `dist${env.dist}`),
      index: "",
      proxy: {
        "/": {
          secure: false,
          target: env.target || process.env.TARGET,
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
            replacement: function(match, p) {
              return `${match}\nimport { fakeMetricsDataGenerationMiddleware } from "src/test-utils/fakeMetricsDataGenerationMiddleware";`;
            },
          },
          {
            pattern: /(?<=applyMiddleware\((.*))\),$/gm,
            replacement: function(match) {
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
