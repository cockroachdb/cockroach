# ttycolor
Conditionally expose ANSI color codes for use on a terminal

This code is suitable for use in CockroachDB as it satisfies the following requirements:

1. it should abstract the color numeric codes into symbolic names
2. it must test the right file descriptor (either stderr or stdout depending on caller requirements)
3. it must be conservative about which color codes are sent to which terminal type. Not every terminal supports the same colors.

Other Go packages for terminal colors typically fail to satisfy requirements 2 or 3, or both.
