// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const { validate } = require("schema-utils");

const { cleanDestinationPaths, tildeify } = require("../util");

const PLUGIN_NAME = `CopyEmittedFilesPlugin`;

const SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    destinations: {
      description: "Destination directories to copy emitted files to.",
      type: "array",
      items: {
        type: "string",
      },
      // Disallow duplicates.
      uniqueItems: true,
    },
  },
};

/**
 * A webpack plugin that copies emitted files to additional directories.
 * Its main purpose is to allow a watch-mode compilation to continuously "push"
 * output files into external directories. This avoids the "multiple/mismatched
 * copies of dependency $foo" behavior that often comes with symlink-based
 * "pull" solutions like 'pnpm link'.
 */
class CopyEmittedFilesPlugin {
  constructor(options) {
    validate(SCHEMA, options, {
      name: PLUGIN_NAME,
      baseDataPath: "options",
    });
    this.options = options;
  }

  /**
   * The implementation of the plugin. Runs one time once webpack starts up,
   * setting up event listeners for various webpack events.
   * @see https://webpack.js.org/contribute/writing-a-plugin/#creating-a-plugin
   */
  apply(compiler) {
    // If no destinations have been configured or if this isn't a watch-mode
    // build, this plugin should do nothing.
    // It's therefore safe to keep an instance of this plugin in the 'Plugins'
    // array, as it'll have no impact on unconfigured builds.
    if (this.options.destinations.length === 0 || !compiler.options.watch) {
      return;
    }

    const logger = compiler.getInfrastructureLogger(PLUGIN_NAME);

    // Sanitize provided paths to ensure they point to a reasonable version of
    // cluster-ui.
    const destinations = cleanDestinationPaths(this.options.destinations, logger);
    logger.info("Emitted files will be copied to:");
    for (const dst of destinations) {
      logger.info("  " + tildeify(dst));
    }

    const relOutputPath = path.relative(compiler.context, compiler.options.output.path);
    // Clear destination areas and recreate output directory structure in each
    // to ensure destinations all match the local output tree.
    // @see https://v4.webpack.js.org/api/compiler-hooks/#afterenvironment
    compiler.hooks.afterEnvironment.tap(PLUGIN_NAME, () => {
      logger.warn("Deleting destinations in preparation for copied files:");
      for (const dst of destinations) {
        // Use lstat to avoid resolving the possible symbolic link.
        const stat = fs.lstatSync(dst);
        const jsDir = path.join(dst, "js");

        if (stat.isDirectory() && fs.existsSync(jsDir)) {
          // Since the destination is already a directory, it's likely been
          // created by webpack or typescript already. Don't remove the entire
          // directory --- only remove the js subtree.
          logger.warn(`  rm -r ${tildeify(jsDir)}`);
          fs.rmSync(jsDir, { recursive: true });
        } else if (stat.isSymbolicLink()) {
          // Since the destination is a symlink, just unlink it.
          logger.warn(`  unlink ${tildeify(dst)}`);
          fs.unlinkSync(dst);
        } else {
          // Avoid weird "deleting destinations" message with no destinations
          // listed. :)
          logger.info(`  ${dst} OK`);
        }

        // Ensure the destination directory and package.json exist.
        logger.debug(`mkdir -p ${path.join(dst, relOutputPath)}`);
        fs.mkdirSync(path.join(dst, relOutputPath), { recursive: true });

        logger.debug(`cp package.json ${path.join(dst, "package.json")}`);
        fs.copyFileSync(
          path.join(compiler.context, "package.json"),
          path.join(dst, "package.json"),
        );
      }
    });

    // Copy files to each destination as they're emitted.
    // @see https://v4.webpack.js.org/api/compiler-hooks/#assetemitted
    compiler.hooks.assetEmitted.tapPromise(PLUGIN_NAME, (file) => {
      return Promise.all(
        destinations.map(async (dstBase) => {
          const src = path.join(relOutputPath, file);
          const dst = path.join(dstBase, relOutputPath, file);
          const dstDir = path.dirname(dst);
          // Ensure destination directories actually exist before copying.
          await fsp.mkdir(dstDir, { recursive: true });
          logger.info(`cp ${src} ${tildeify(dstDir)}`);
          return fsp.copyFile(src, dst);
        }),
      );
    });
  }
}

module.exports.CopyEmittedFilesPlugin = CopyEmittedFilesPlugin;
