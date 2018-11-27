// Since we link against C++ libraries, like RocksDB and libroach, we need to
// link against the C++ standard library. This presence of this file convinces
// cgo to link this package using the C++ compiler instead of the C compiler,
// which brings in the appropriate, platform-specific C++ library (e.g., libc++
// on macOS or libstdc++ on Linux).
