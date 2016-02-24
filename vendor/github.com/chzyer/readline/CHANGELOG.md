# ChangeLog

### 1.1 - 2015-11-20

* #12 Add support for key `<Delete>`/`<Home>`/`<End>`
* Only enter raw mode as needed (calling `Readline()`), program will receive signal(e.g. Ctrl+C) if not interact with `readline`.
* Bugs fixed for `PrefixCompleter`
* Press `Ctrl+D` in empty line will cause `io.EOF` in error, Press `Ctrl+C` in anytime will cause `ErrInterrupt` instead of `io.EOF`, this will privodes a shell-like user experience.
* Customable Interrupt/EOF prompt in `Config`
* #17 Change atomic package to use 32bit function to let it runnable on arm 32bit devices
* Provides a new password user experience(`readline.ReadPasswordEx()`).

### 1.0 - 2015-10-14

* Initial public release.
