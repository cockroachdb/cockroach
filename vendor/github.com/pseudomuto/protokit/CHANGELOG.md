# Changelog

All noteworthy changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/pseudomuto/protokit/compare/v0.2.0...master)

## [0.2.0](https://github.com/pseudomuto/protokit/compare/v0.1.0...v0.2.0)

### Added

* Support for extended options in files, services, methods, enums, enum values, messages and fields. For example, the [`google.api.http` method option](https://cloud.google.com/service-infrastructure/docs/service-management/reference/rpc/google.api#httprule).

## [0.1.0](https://github.com/pseudomuto/protokit/compare/v0.1.0-pre3...v0.1.0)

### Added

* `(Get)PackageComments` and `(Get)SyntaxComments` to `FileDescriptor`

### Deprecated

* `Comments` and `GetComments()` from `FileDescriptor`

## [0.1.0-pre3](https://github.com/pseudomuto/protokit/compare/v0.1.0-pre2...v0.1.0-pre3)

### Added

* `FileDescriptor{}.Imported` for referencing publically imported descriptors, enums, and extensions
* `Comments{}.Get` to create empty comments (rather than `nil`) for all objects when a comment isn't defined.

## [0.1.0-pre2](https://github.com/pseudomuto/protokit/compare/v0.1.0-pre...v0.1.0-pre2) - 2018-02-22

### Fixed

* CreateGenRequest should filter for FileToGenerate using the `GetName()` rather than getting the base name

## [0.1.0-pre](https://github.com/pseudomuto/protokit/compare/c3aa082037b33bcd713106641e88afba846d003d...v0.1.0-pre) - 2018-02-21

The initial alpha release of protokit.
