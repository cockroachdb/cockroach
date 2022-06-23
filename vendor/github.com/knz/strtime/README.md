# strtime: stable strptime / strftime for Go

`strftime` and `strptime` are functions found in nearly all C
library implementations but with slightly different behaviors on each
platform.

To provide identical behavior on all platforms where Go is supported,
this **strtime** package provides two functions `Strftime` and
`Strptime` which can be used *in lieu* of the platform's native C
implementation.

To achieve this **strtime**:

- embeds and extends a `Strftime` function in Go, using an
  implementation originally from `leekchan`'s
  [timeutil](https://github.com/leekchan/timeutil) package, which
  performs the format conversion natively in Go and is licensed under
  the terms of the standard MIT license; and
- embeds the standard FreeBSD implementation of `strptime` in
  C. FreeBSD's implementation is mature, robust, has the widest format
  specifier coverage, and its code is reusable under the terms of the
  permissive BSD license.

**strtime** is itself offered under the terms of the 2-clause BSD license.

# How to use

~~~ go
func Strptime(value string, layout string) (time.Time, error)
func Strftime(t time.Time, layout string) (string, error)
~~~

Examples:

~~~ go
   start := "2016-10-20"
   t, _ := strtime.Strptime(start, "%Y-%m-%d")
   end, _ := strtime.Strftime(t, "%Y-%m-%d")
   Fmt.Println(start, end)
~~~

# Supported format specifiers

| Format | Description | Notes
|--------|-------------|---------
| `%a` | Short week day ("mon", "tue", etc) |
| `%A` | Long week day ("monday", "tuesday", etc) |
| `%b` | Short month name ("jan", "feb" etc) |
| `%B` | Long month name ("january", "february" etc) |
| `%c` | Equivalent to `%a %b %e %H:%M:%S %Y` |
| `%C` | Century | Only reliable for years -9999 to 9999
| `%d` | Day of month 01-31 |
| `%D` | Equivalent to `%m/%d/%y` |
| `%e` | Like `%d` but leading zeros are replaced by a space. |
| `%f` | Fractional part of a second with nanosecond precision, e.g. "`123`" is 123ms; "`123456`" is 123456µs, etc. | `Strftime` always formats using 6 digits.
| `%F` | Equivalent to `%Y-%m-%d` |
| `%h` | Equivalent to `%b` |
| `%H` | Hours 00-23  | See also `%k`
| `%I` | Hours 01-12  | See also `%p`, `%l`
| `%j` | Day of year 000-366 |
| `%k` | Hours 0-23 (padded with spaces) | See also `%H`
| `%l` | Hours 1-12 (padded with spaces) | See also `%I`
| `%m` | Month 01-12 |
| `%M` | Minutes 00-59 |
| `%n` | A newline character |
| `%p` | AM/PM | Only valid when placed after hour-related specifiers. See also `%I`, `%l`
| `%r` | Equivalent to `%I:%M:%S %p` |
| `%R` | Equivalent to `%H:%M` | See also `%T`
| `%s` | Number of seconds since 1970-01-01 00:00:00 +0000 (UTC) |
| `%S` | Seconds 00-59 |
| `%t` | A tab character |
| `%T` | Equivalent to `%H:%M:%S` | See also `%R`
| `%u` | The day of the week as a decimal, range 1 to 7, Monday being 1 | See also `%w`
| `%U` | The week number of the current year as a decimal number, range 00 to 53, starting with the first Sunday as the first day of week 01 | See also `%W`
| `%w` | The day of the week as a decimal, range 0 to 6, Sunday being 1 | See also `%u`
| `%W` | The week number of the current year as a decimal number, range 00 to 53, starting with the first Monday as the first day of week 01 | See also `%U`
| `%x` | Equivalent to `%D` |
| `%X` | Equivalent to `%T` |
| `%y` | Year without a century 00-99 | Years 00-68 are 2000-2068
| `%Y` | Year including the century |
| `%z` | Time zone offset +/-NNNN | `Strftime` always prints `+0000`
| `%Z` | `UTC` or `GMT` | `Strftime` always prints `UTC`
