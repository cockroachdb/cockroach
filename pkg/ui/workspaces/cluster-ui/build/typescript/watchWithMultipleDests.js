// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

const fs = require("fs");
const path = require("path");

const argv = require("minimist")(process.argv.slice(2));
const ts = require("typescript");

const { cleanDestinationPaths, tildeify } = require("../util");

/**
 * A minimal wrapper around tsc --watch, implemented using the typescript JS API
 * to support writing emitted files to multiple directories.
 * This function never returns, as it hosts a filesystem 'watcher'.
 * @param {Object} opts - a bag of options
 * @param {boolean} opts.interactive - whether to show compiler errors as they occur.
 * @param {string[]} opts.destinations - an array of directories to emit declarations to.
 *                                       The default from tsconfig.json will be automatically
 *                                       prepended to this list.
 * @returns never
 * @see https://github.com/microsoft/TypeScript/wiki/Using-the-Compiler-API/167d197d290bec04b626b91b6f453123ef309e58#writing-an-incremental-program-watcher
 */
function watch(opts) {
  const destinations = opts.destinations;

  // Find a tsconfig.json.
  const configPath = ts.findConfigFile(
    /* searchPath */ "./",
    ts.sys.fileExists,
    "tsconfig.json",
  );
  if (!configPath) {
    throw new Error("Could not find a valid 'tsconfig.json'");
  }

  // Create a wrapper around the default ts.sys.writeFile that also writes files
  // to each destination.
  const tsSysWriteFile = ts.sys.writeFile;
  /** Wraps ts.sys.writeFile to write files to multiple destinations. */
  function writeFile(fileName, data, writeByteOrderMark) {
    // First, write to the intended path. Providing a writeFile function
    // means TypeScript won't do this on its own.
    tsSysWriteFile(fileName, data, writeByteOrderMark);

    // Get a path to fileName relative to the root of this package.
    // Luckily, configPath is always the absolute path to a file at the root.
    const relPath = path.relative(ts.sys.getCurrentDirectory(), fileName);
    for (const dst of destinations) {
      const absDstPath = path.join(dst, relPath);
      tsSysWriteFile(absDstPath, data, writeByteOrderMark);
    }
  }

  const reportWatchStatus = opts.interactive
    ? undefined // default output
    : () => {}; // nop output

  // Create a watching compiler host that we'll pass to ts.createWatchProgram
  // later.
  const host = ts.createWatchCompilerHost(
    configPath,
    /* optionsToExtend */ {},
    {
      ...ts.sys,
      // Never clear the screen, to simulate tsc --watch --preserveWatchOutput.
      clearScreen: undefined,
      writeFile,
    },
    ts.createEmitAndSemanticDiagnosticsBuilderProgram,
    // Use the default diagnostic reporter.
    /* reportDiagnostic */ undefined,
    reportWatchStatus,
  );

  // Create a wrapper around the default createProgram hook (called whenever a
  // compilation pass starts) to log a helpful message every time in interactive
  // mode and just once otherwise.
  const origCreateProgram = host.createProgram;
  let createProgramHasRun = false;
  host.createProgram = (rootNames, options, host, oldProgram, configFileParsingDiagnostics, projectReferences) => {
    if (opts.interactive || !createProgramHasRun) {
      createProgramHasRun = true;

      const compilerOptions = options || {};
      const currentDir = host.getCurrentDirectory();
      // Compute the declaration directory relative to the project root.
      const relDeclarationDir = path.relative(
        currentDir,
        compilerOptions.declarationDir || compilerOptions.outDir || ""
      );

      console.log("TypeScript declarations will be silently written to:")
      for (const dst of ["./"].concat(destinations)) {
        console.log(`  ${tildeify(path.join(dst, relDeclarationDir))}`);
      }
    }

    return origCreateProgram(rootNames, options, host, oldProgram, configFileParsingDiagnostics, projectReferences);
  }

  // Create an initial program, watch files, and incrementally update that
  // program object.
  ts.createWatchProgram(host);
}

const isHelp = argv.h || argv.help;
const hasPositionalArgs = argv._.length !== 0;
if (isHelp || hasPositionalArgs) {
  const argv1 = path.relative(path.join(__dirname, "../../"), process.argv[1]);
  const help = `
${argv1} - a minimal replacement for 'tsc --watch' that copies generated files to extra directories.

Usage:
  ${argv1} [--no-interactive] [--copy-to DIR]...

Flags:
  --no-interactive print TS compiler errors if they occur and clear the terminal
                   between compilations.
  --copy-to DIR    path to copy emitted files to, in addition to the default in
                   tsconfig.json. Can be specified multiple times.
  -h, --help       prints this message
  `;

  if (hasPositionalArgs) {
    console.error("Unexpected positional arguments:", argv._);
    console.error();
  }
  console.error(help.trim());
  process.exit(hasPositionalArgs ? 1 : 0);
}

const copyToArgs = argv["copy-to"];
const destinations = typeof copyToArgs === "string"
  ? [ copyToArgs ]
  : (copyToArgs || []);

watch({
  interactive: argv.interactive ?? true,
  destinations: cleanDestinationPaths(destinations),
});
