# elastic/gosigar Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Fixed

### Changed

### Deprecated

## [0.14.1]

### Fixed

- Fix unsupported devices for filesystem. [#159](https://github.com/elastic/gosigar/pull/159)

## [0.14.0]

### Addded

- Add darwin ARM support. [#152](https://github.com/elastic/gosigar/pull/152)

## [0.13.0]

### Added

- Add method of overriding the /sys/fs/cgroup hierarchy, for reading cgroup metrics inside Docker [#148](https://github.com/elastic/gosigar/pull/148)

## [0.12.0]

### Added

- Add `Cached` data to Memory [#145](https://github.com/elastic/gosigar/pull/145)
- Add `SysTypeName` support on Windows [#146](https://github.com/elastic/gosigar/pull/146)

## [0.11.0]

### Added

- Added support for AIX. [#133](https://github.com/elastic/gosigar/pull/133)

### Fixed

- Fixed the `ss` example by replacing the Logrus package with the stdlib `log` package. [#123](https://github.com/elastic/gosigar/issues/123) [#136](https://github.com/elastic/gosigar/pull/136)
- Replaced `bytePtrToString` and cleaned up darwin code. [#138](https://github.com/elastic/gosigar/issues/138) [#141](https://github.com/elastic/gosigar/pull/141)

## [0.10.5]

### Fixed

- Fixed uptime calculation under Windows. [#126](https://github.com/elastic/gosigar/pull/126)
- Fixed compilation issue for darwin/386. [#128](https://github.com/elastic/gosigar/pull/128)

### Changed

- Load DLLs only from Windows system directory. [#132](https://github.com/elastic/gosigar/pull/132)

## [0.10.4]

### Fixed

- Fixed a crash when splitting command-line arguments under Windows. [#124](https://github.com/elastic/gosigar/pull/124)

## [0.10.3]

### Fixed
- ProcState.Get() doesn't fail under Windows when it cannot obtain process ownership information. [#121](https://github.com/elastic/gosigar/pull/121)

## [0.10.2]

### Fixed
- Fix memory leak when getting process arguments. [#119](https://github.com/elastic/gosigar/pull/119)

## [0.10.1]

### Fixed
- Replaced the WMI queries with win32 apis due to high CPU usage. [#116](https://github.com/elastic/gosigar/pull/116)

## [0.10.0]

### Added
- List filesystems on Windows that have an access path but not an assigned letter. [#112](https://github.com/elastic/gosigar/pull/112)

### Fixed
- Added missing runtime import for FreeBSD. [#104](https://github.com/elastic/gosigar/pull/104)
- Handle nil command line in Windows processes. [#110](https://github.com/elastic/gosigar/pull/110)

## [0.9.0]

### Added
- Added support for huge TLB pages on Linux [#97](https://github.com/elastic/gosigar/pull/97)  
- Added support for big endian platform [#100](https://github.com/elastic/gosigar/pull/100) 

### Fixed
- Add missing method for OpenBSD [#99](https://github.com/elastic/gosigar/pull/99)

## [0.8.0]

### Added
- Added partial `getrusage` support for Windows to retrieve system CPU time and user CPU time. [#95](https://github.com/elastic/gosigar/pull/95)
- Added full `getrusage` support for Unix. [#95](https://github.com/elastic/gosigar/pull/95)

## [0.7.0]

### Added
- Added method stubs for process handling for operating system that are not supported
  by gosigar. All methods return `ErrNotImplemented` on such systems. [#88](https://github.com/elastic/gosigar/pull/88)

### Fixed
- Fix freebsd build by using the common version of Get(pid). [#91](https://github.com/elastic/gosigar/pull/91)

### Changed
- Fixed issues in cgroup package by adding missing error checks and closing
  file handles. [#92](https://github.com/elastic/gosigar/pull/92)

## [0.6.0]

### Added
- Added method stubs to enable compilation for operating systems that are not
  supported by gosigar. All methods return `ErrNotImplemented` on these unsupported
  operating systems. [#83](https://github.com/elastic/gosigar/pull/83)
- FreeBSD returns `ErrNotImplemented` for `ProcTime.Get`. [#83](https://github.com/elastic/gosigar/pull/83)

### Changed
- OpenBSD returns `ErrNotImplemented` for `ProcTime.Get` instead of `nil`. [#83](https://github.com/elastic/gosigar/pull/83)
- Fixed incorrect `Mem.Used` calculation under linux. [#82](https://github.com/elastic/gosigar/pull/82)
- Fixed `ProcState` on Linux and FreeBSD when process names contain parentheses. [#81](https://github.com/elastic/gosigar/pull/81)

### Removed
- Remove NetBSD build from sigar_unix.go as it is not supported by gosigar. [#83](https://github.com/elastic/gosigar/pull/83)

## [0.5.0]

### Changed
- Fixed Trim environment variables when comparing values in the test suite. [#79](https://github.com/elastic/gosigar/pull/79)
- Make `kern_procargs` more robust under darwin when we cannot retrieve
  all the information about a process. [#78](https://github.com/elastic/gosigar/pull/78)

## [0.4.0]

### Changed
- Fixed Windows issue that caused a hang during `init()` if WMI wasn't ready. [#74](https://github.com/elastic/gosigar/pull/74)

## [0.3.0]

### Added
- Read `MemAvailable` value for kernel 3.14+ [#71](https://github.com/elastic/gosigar/pull/71)

## [0.2.0]

### Added
- Added `ErrCgroupsMissing` to indicate that /proc/cgroups is missing which is
  an indicator that cgroups were disabled at compile time. [#64](https://github.com/elastic/gosigar/pull/64)

### Changed
- Changed `cgroup.SupportedSubsystems()` to honor the "enabled" column in the
  /proc/cgroups file. [#64](https://github.com/elastic/gosigar/pull/64)

## [0.1.0]

### Added
- Added `CpuList` implementation for Windows that returns CPU timing information
  on a per CPU basis. [#55](https://github.com/elastic/gosigar/pull/55)
- Added `Uptime` implementation for Windows. [#55](https://github.com/elastic/gosigar/pull/55)
- Added `Swap` implementation for Windows based on page file metrics. [#55](https://github.com/elastic/gosigar/pull/55)
- Added support to `github.com/gosigar/sys/windows` for querying and enabling
  privileges in a process token.
- Added utility code for interfacing with linux NETLINK_INET_DIAG. [#60](https://github.com/elastic/gosigar/pull/60)
- Added `ProcEnv` for getting a process's environment variables. [#61](https://github.com/elastic/gosigar/pull/61)

### Changed
- Changed several `OpenProcess` calls on Windows to request the lowest possible
  access privileges. [#50](https://github.com/elastic/gosigar/pull/50)
- Removed cgo usage from Windows code.
- Added OS version checks to `ProcArgs.Get` on Windows because the
  `Win32_Process` WMI query is not available prior to Windows vista. On XP and
  Windows 2003, this method returns `ErrNotImplemented`. [#55](https://github.com/elastic/gosigar/pull/55)

### Fixed
- Fixed value of `Mem.ActualFree` and `Mem.ActualUsed` on Windows. [#49](https://github.com/elastic/gosigar/pull/49)
- Fixed `ProcTime.StartTime` on Windows to report value in milliseconds since
  Unix epoch. [#51](https://github.com/elastic/gosigar/pull/51)
- Fixed `ProcStatus.PPID` value is wrong on Windows. [#55](https://github.com/elastic/gosigar/pull/55)
- Fixed `ProcStatus.Username` error on Windows XP [#56](https://github.com/elastic/gosigar/pull/56)

[Unreleased]: https://github.com/elastic/gosigar/compare/v0.14.1...HEAD
[0.14.1]: https://github.com/elastic/gosigar/releases/tag/v0.14.1
[0.14.0]: https://github.com/elastic/gosigar/releases/tag/v0.14.0
[0.13.0]: https://github.com/elastic/gosigar/releases/tag/v0.13.0
[0.12.0]: https://github.com/elastic/gosigar/releases/tag/v0.12.0
[0.11.0]: https://github.com/elastic/gosigar/releases/tag/v0.11.0
[0.10.5]: https://github.com/elastic/gosigar/releases/tag/v0.10.5
[0.10.4]: https://github.com/elastic/gosigar/releases/tag/v0.10.4
[0.10.3]: https://github.com/elastic/gosigar/releases/tag/v0.10.3
[0.10.2]: https://github.com/elastic/gosigar/releases/tag/v0.10.2
[0.10.1]: https://github.com/elastic/gosigar/releases/tag/v0.10.1
[0.10.0]: https://github.com/elastic/gosigar/releases/tag/v0.10.0
[0.9.0]: https://github.com/elastic/gosigar/releases/tag/v0.9.0
[0.8.0]: https://github.com/elastic/gosigar/releases/tag/v0.8.0
[0.7.0]: https://github.com/elastic/gosigar/releases/tag/v0.7.0
[0.6.0]: https://github.com/elastic/gosigar/releases/tag/v0.6.0
[0.5.0]: https://github.com/elastic/gosigar/releases/tag/v0.5.0
[0.4.0]: https://github.com/elastic/gosigar/releases/tag/v0.4.0
[0.3.0]: https://github.com/elastic/gosigar/releases/tag/v0.3.0
[0.2.0]: https://github.com/elastic/gosigar/releases/tag/v0.2.0
[0.1.0]: https://github.com/elastic/gosigar/releases/tag/v0.1.0
