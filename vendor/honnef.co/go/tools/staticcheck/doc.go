// Package staticcheck contains analyzes that find bugs and performance issues.
// Barring the rare false positive, any code flagged by these analyzes needs to be fixed.
package staticcheck

import "honnef.co/go/tools/analysis/lint"

var Docs = lint.Markdownify(map[string]*lint.RawDocumentation{
	"SA1000": {
		Title:    `Invalid regular expression`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1001": {
		Title:    `Invalid template`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1002": {
		Title:    `Invalid format in \'time.Parse\'`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1003": {
		Title: `Unsupported argument to functions in \'encoding/binary\'`,
		Text: `The \'encoding/binary\' package can only serialize types with known sizes.
This precludes the use of the \'int\' and \'uint\' types, as their sizes
differ on different architectures. Furthermore, it doesn't support
serializing maps, channels, strings, or functions.

Before Go 1.8, \'bool\' wasn't supported, either.`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1004": {
		Title: `Suspiciously small untyped constant in \'time.Sleep\'`,
		Text: `The \'time\'.Sleep function takes a \'time.Duration\' as its only argument.
Durations are expressed in nanoseconds. Thus, calling \'time.Sleep(1)\'
will sleep for 1 nanosecond. This is a common source of bugs, as sleep
functions in other languages often accept seconds or milliseconds.

The \'time\' package provides constants such as \'time.Second\' to express
large durations. These can be combined with arithmetic to express
arbitrary durations, for example \'5 * time.Second\' for 5 seconds.

If you truly meant to sleep for a tiny amount of time, use
\'n * time.Nanosecond\' to signal to Staticcheck that you did mean to sleep
for some amount of nanoseconds.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1005": {
		Title: `Invalid first argument to \'exec.Command\'`,
		Text: `\'os/exec\' runs programs directly (using variants of the fork and exec
system calls on Unix systems). This shouldn't be confused with running
a command in a shell. The shell will allow for features such as input
redirection, pipes, and general scripting. The shell is also
responsible for splitting the user's input into a program name and its
arguments. For example, the equivalent to

    ls / /tmp

would be

    exec.Command("ls", "/", "/tmp")

If you want to run a command in a shell, consider using something like
the following – but be aware that not all systems, particularly
Windows, will have a \'/bin/sh\' program:

    exec.Command("/bin/sh", "-c", "ls | grep Awesome")`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1006": {
		Title: `\'Printf\' with dynamic first argument and no further arguments`,
		Text: `Using \'fmt.Printf\' with a dynamic first argument can lead to unexpected
output. The first argument is a format string, where certain character
combinations have special meaning. If, for example, a user were to
enter a string such as

    Interest rate: 5%

and you printed it with

    fmt.Printf(s)

it would lead to the following output:

    Interest rate: 5%!(NOVERB).

Similarly, forming the first parameter via string concatenation with
user input should be avoided for the same reason. When printing user
input, either use a variant of \'fmt.Print\', or use the \'%s\' Printf verb
and pass the string as an argument.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1007": {
		Title:    `Invalid URL in \'net/url.Parse\'`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1008": {
		Title: `Non-canonical key in \'http.Header\' map`,
		Text: `Keys in \'http.Header\' maps are canonical, meaning they follow a specific
combination of uppercase and lowercase letters. Methods such as
\'http.Header.Add\' and \'http.Header.Del\' convert inputs into this canonical
form before manipulating the map.

When manipulating \'http.Header\' maps directly, as opposed to using the
provided methods, care should be taken to stick to canonical form in
order to avoid inconsistencies. The following piece of code
demonstrates one such inconsistency:

    h := http.Header{}
    h["etag"] = []string{"1234"}
    h.Add("etag", "5678")
    fmt.Println(h)

    // Output:
    // map[Etag:[5678] etag:[1234]]

The easiest way of obtaining the canonical form of a key is to use
\'http.CanonicalHeaderKey\'.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1010": {
		Title: `\'(*regexp.Regexp).FindAll\' called with \'n == 0\', which will always return zero results`,
		Text: `If \'n >= 0\', the function returns at most \'n\' matches/submatches. To
return all results, specify a negative number.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny, // MergeIfAny if we only flag literals, not named constants
	},

	"SA1011": {
		Title:    `Various methods in the \"strings\" package expect valid UTF-8, but invalid input is provided`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1012": {
		Title:    `A nil \'context.Context\' is being passed to a function, consider using \'context.TODO\' instead`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1013": {
		Title:    `\'io.Seeker.Seek\' is being called with the whence constant as the first argument, but it should be the second`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1014": {
		Title:    `Non-pointer value passed to \'Unmarshal\' or \'Decode\'`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1015": {
		Title:    `Using \'time.Tick\' in a way that will leak. Consider using \'time.NewTicker\', and only use \'time.Tick\' in tests, commands and endless functions`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1016": {
		Title: `Trapping a signal that cannot be trapped`,
		Text: `Not all signals can be intercepted by a process. Specifically, on
UNIX-like systems, the \'syscall.SIGKILL\' and \'syscall.SIGSTOP\' signals are
never passed to the process, but instead handled directly by the
kernel. It is therefore pointless to try and handle these signals.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1017": {
		Title: `Channels used with \'os/signal.Notify\' should be buffered`,
		Text: `The \'os/signal\' package uses non-blocking channel sends when delivering
signals. If the receiving end of the channel isn't ready and the
channel is either unbuffered or full, the signal will be dropped. To
avoid missing signals, the channel should be buffered and of the
appropriate size. For a channel used for notification of just one
signal value, a buffer of size 1 is sufficient.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1018": {
		Title: `\'strings.Replace\' called with \'n == 0\', which does nothing`,
		Text: `With \'n == 0\', zero instances will be replaced. To replace all
instances, use a negative number, or use \'strings.ReplaceAll\'.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny, // MergeIfAny if we only flag literals, not named constants
	},

	"SA1019": {
		Title:    `Using a deprecated function, variable, constant or field`,
		Since:    "2017.1",
		Severity: lint.SeverityDeprecated,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1020": {
		Title:    `Using an invalid host:port pair with a \'net.Listen\'-related function`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1021": {
		Title: `Using \'bytes.Equal\' to compare two \'net.IP\'`,
		Text: `A \'net.IP\' stores an IPv4 or IPv6 address as a slice of bytes. The
length of the slice for an IPv4 address, however, can be either 4 or
16 bytes long, using different ways of representing IPv4 addresses. In
order to correctly compare two \'net.IP\'s, the \'net.IP.Equal\' method should
be used, as it takes both representations into account.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1023": {
		Title:    `Modifying the buffer in an \'io.Writer\' implementation`,
		Text:     `\'Write\' must not modify the slice data, even temporarily.`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1024": {
		Title: `A string cutset contains duplicate characters`,
		Text: `The \'strings.TrimLeft\' and \'strings.TrimRight\' functions take cutsets, not
prefixes. A cutset is treated as a set of characters to remove from a
string. For example,

    strings.TrimLeft("42133word", "1234")

will result in the string \'"word"\' – any characters that are 1, 2, 3 or
4 are cut from the left of the string.

In order to remove one string from another, use \'strings.TrimPrefix\' instead.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1025": {
		Title:    `It is not possible to use \'(*time.Timer).Reset\''s return value correctly`,
		Since:    "2019.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1026": {
		Title:    `Cannot marshal channels or functions`,
		Since:    "2019.2",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1027": {
		Title: `Atomic access to 64-bit variable must be 64-bit aligned`,
		Text: `On ARM, x86-32, and 32-bit MIPS, it is the caller's responsibility to
arrange for 64-bit alignment of 64-bit words accessed atomically. The
first word in a variable or in an allocated struct, array, or slice
can be relied upon to be 64-bit aligned.

You can use the structlayout tool to inspect the alignment of fields
in a struct.`,
		Since:    "2019.2",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1028": {
		Title:    `\'sort.Slice\' can only be used on slices`,
		Text:     `The first argument of \'sort.Slice\' must be a slice.`,
		Since:    "2020.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1029": {
		Title: `Inappropriate key in call to \'context.WithValue\'`,
		Text: `The provided key must be comparable and should not be
of type \'string\' or any other built-in type to avoid collisions between
packages using context. Users of \'WithValue\' should define their own
types for keys.

To avoid allocating when assigning to an \'interface{}\',
context keys often have concrete type \'struct{}\'. Alternatively,
exported context key variables' static type should be a pointer or
interface.`,
		Since:    "2020.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA1030": {
		Title: `Invalid argument in call to a \'strconv\' function`,
		Text: `This check validates the format, number base and bit size arguments of
the various parsing and formatting functions in \'strconv\'.`,
		Since:    "2021.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA2000": {
		Title:    `\'sync.WaitGroup.Add\' called inside the goroutine, leading to a race condition`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA2001": {
		Title: `Empty critical section, did you mean to defer the unlock?`,
		Text: `Empty critical sections of the kind

    mu.Lock()
    mu.Unlock()

are very often a typo, and the following was intended instead:

    mu.Lock()
    defer mu.Unlock()

Do note that sometimes empty critical sections can be useful, as a
form of signaling to wait on another goroutine. Many times, there are
simpler ways of achieving the same effect. When that isn't the case,
the code should be amply commented to avoid confusion. Combining such
comments with a \'//lint:ignore\' directive can be used to suppress this
rare false positive.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA2002": {
		Title:    `Called \'testing.T.FailNow\' or \'SkipNow\' in a goroutine, which isn't allowed`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA2003": {
		Title:    `Deferred \'Lock\' right after locking, likely meant to defer \'Unlock\' instead`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA3000": {
		Title: `\'TestMain\' doesn't call \'os.Exit\', hiding test failures`,
		Text: `Test executables (and in turn \"go test\") exit with a non-zero status
code if any tests failed. When specifying your own \'TestMain\' function,
it is your responsibility to arrange for this, by calling \'os.Exit\' with
the correct code. The correct code is returned by \'(*testing.M).Run\', so
the usual way of implementing \'TestMain\' is to end it with
\'os.Exit(m.Run())\'.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA3001": {
		Title: `Assigning to \'b.N\' in benchmarks distorts the results`,
		Text: `The testing package dynamically sets \'b.N\' to improve the reliability of
benchmarks and uses it in computations to determine the duration of a
single operation. Benchmark code must not alter \'b.N\' as this would
falsify results.`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4000": {
		Title:    `Binary operator has identical expressions on both sides`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4001": {
		Title:    `\'&*x\' gets simplified to \'x\', it does not copy \'x\'`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4003": {
		Title:    `Comparing unsigned values against negative values is pointless`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4004": {
		Title:    `The loop exits unconditionally after one iteration`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4005": {
		Title:    `Field assignment that will never be observed. Did you mean to use a pointer receiver?`,
		Since:    "2021.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4006": {
		Title:    `A value assigned to a variable is never read before being overwritten. Forgotten error check or dead code?`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4008": {
		Title:    `The variable in the loop condition never changes, are you incrementing the wrong variable?`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4009": {
		Title:    `A function argument is overwritten before its first use`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4010": {
		Title:    `The result of \'append\' will never be observed anywhere`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4011": {
		Title:    `Break statement with no effect. Did you mean to break out of an outer loop?`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4012": {
		Title:    `Comparing a value against NaN even though no value is equal to NaN`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4013": {
		Title:    `Negating a boolean twice (\'!!b\') is the same as writing \'b\'. This is either redundant, or a typo.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4014": {
		Title:    `An if/else if chain has repeated conditions and no side-effects; if the condition didn't match the first time, it won't match the second time, either`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4015": {
		Title:    `Calling functions like \'math.Ceil\' on floats converted from integers doesn't do anything useful`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4016": {
		Title:    `Certain bitwise operations, such as \'x ^ 0\', do not do anything useful`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny, // MergeIfAny if we only flag literals, not named constants
	},

	"SA4017": {
		Title:    `A pure function's return value is discarded, making the call pointless`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4018": {
		Title:    `Self-assignment of variables`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4019": {
		Title:    `Multiple, identical build constraints in the same file`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4020": {
		Title: `Unreachable case clause in a type switch`,
		Text: `In a type switch like the following

    type T struct{}
    func (T) Read(b []byte) (int, error) { return 0, nil }

    var v interface{} = T{}

    switch v.(type) {
    case io.Reader:
        // ...
    case T:
        // unreachable
    }

the second case clause can never be reached because \'T\' implements
\'io.Reader\' and case clauses are evaluated in source order.

Another example:

    type T struct{}
    func (T) Read(b []byte) (int, error) { return 0, nil }
    func (T) Close() error { return nil }

    var v interface{} = T{}

    switch v.(type) {
    case io.Reader:
        // ...
    case io.ReadCloser:
        // unreachable
    }

Even though \'T\' has a \'Close\' method and thus implements \'io.ReadCloser\',
\'io.Reader\' will always match first. The method set of \'io.Reader\' is a
subset of \'io.ReadCloser\'. Thus it is impossible to match the second
case without matching the first case.


Structurally equivalent interfaces

A special case of the previous example are structurally identical
interfaces. Given these declarations

    type T error
    type V error

    func doSomething() error {
        err, ok := doAnotherThing()
        if ok {
            return T(err)
        }

        return U(err)
    }

the following type switch will have an unreachable case clause:

    switch doSomething().(type) {
    case T:
        // ...
    case V:
        // unreachable
    }

\'T\' will always match before V because they are structurally equivalent
and therefore \'doSomething()\''s return value implements both.`,
		Since:    "2019.2",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA4021": {
		Title:    `\"x = append(y)\" is equivalent to \"x = y\"`,
		Since:    "2019.2",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4022": {
		Title:    `Comparing the address of a variable against nil`,
		Text:     `Code such as \"if &x == nil\" is meaningless, because taking the address of a variable always yields a non-nil pointer.`,
		Since:    "2020.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4023": {
		Title: `Impossible comparison of interface value with untyped nil`,
		Text: `Under the covers, interfaces are implemented as two elements, a
type T and a value V. V is a concrete value such as an int,
struct or pointer, never an interface itself, and has type T. For
instance, if we store the int value 3 in an interface, the
resulting interface value has, schematically, (T=int, V=3). The
value V is also known as the interface's dynamic value, since a
given interface variable might hold different values V (and
corresponding types T) during the execution of the program.

An interface value is nil only if the V and T are both
unset, (T=nil, V is not set), In particular, a nil interface will
always hold a nil type. If we store a nil pointer of type *int
inside an interface value, the inner type will be *int regardless
of the value of the pointer: (T=*int, V=nil). Such an interface
value will therefore be non-nil even when the pointer value V
inside is nil.

This situation can be confusing, and arises when a nil value is
stored inside an interface value such as an error return:

    func returnsError() error {
        var p *MyError = nil
        if bad() {
            p = ErrBad
        }
        return p // Will always return a non-nil error.
    }

If all goes well, the function returns a nil p, so the return
value is an error interface value holding (T=*MyError, V=nil).
This means that if the caller compares the returned error to nil,
it will always look as if there was an error even if nothing bad
happened. To return a proper nil error to the caller, the
function must return an explicit nil:

    func returnsError() error {
        if bad() {
            return ErrBad
        }
        return nil
    }

It's a good idea for functions that return errors always to use
the error type in their signature (as we did above) rather than a
concrete type such as \'*MyError\', to help guarantee the error is
created correctly. As an example, \'os.Open\' returns an error even
though, if not nil, it's always of concrete type *os.PathError.

Similar situations to those described here can arise whenever
interfaces are used. Just keep in mind that if any concrete value
has been stored in the interface, the interface will not be nil.
For more information, see The Laws of
Reflection (https://golang.org/doc/articles/laws_of_reflection.html).

This text has been copied from
https://golang.org/doc/faq#nil_error, licensed under the Creative
Commons Attribution 3.0 License.`,
		Since:    "2020.2",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny, // TODO should this be MergeIfAll?
	},

	"SA4024": {
		Title: `Checking for impossible return value from a builtin function`,
		Text: `Return values of the \'len\' and \'cap\' builtins cannot be negative.

See https://golang.org/pkg/builtin/#len and https://golang.org/pkg/builtin/#cap.

Example:

    if len(slice) < 0 {
        fmt.Println("unreachable code")
    }`,
		Since:    "2021.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4025": {
		Title: "Integer division of literals that results in zero",
		Text: `When dividing two integer constants, the result will
also be an integer. Thus, a division such as \'2 / 3\' results in \'0\'.
This is true for all of the following examples:

	_ = 2 / 3
	const _ = 2 / 3
	const _ float64 = 2 / 3
	_ = float64(2 / 3)

Staticcheck will flag such divisions if both sides of the division are
integer literals, as it is highly unlikely that the division was
intended to truncate to zero. Staticcheck will not flag integer
division involving named constants, to avoid noisy positives.
`,
		Since:    "2021.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4026": {
		Title: "Go constants cannot express negative zero",
		Text: `In IEEE 754 floating point math, zero has a sign and can be positive
or negative. This can be useful in certain numerical code.

Go constants, however, cannot express negative zero. This means that
the literals \'-0.0\' and \'0.0\' have the same ideal value (zero) and
will both represent positive zero at runtime.

To explicitly and reliably create a negative zero, you can use the
\'math.Copysign\' function: \'math.Copysign(0, -1)\'.`,
		Since:    "2021.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4027": {
		Title: `\'(*net/url.URL).Query\' returns a copy, modifying it doesn't change the URL`,
		Text: `\'(*net/url.URL).Query\' parses the current value of \'net/url.URL.RawQuery\'
and returns it as a map of type \'net/url.Values\'. Subsequent changes to
this map will not affect the URL unless the map gets encoded and
assigned to the URL's \'RawQuery\'.

As a consequence, the following code pattern is an expensive no-op:
\'u.Query().Add(key, value)\'.`,
		Since:    "2021.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4028": {
		Title:    `\'x % 1\' is always zero`,
		Since:    "2022.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny, // MergeIfAny if we only flag literals, not named constants
	},

	"SA4029": {
		Title: "Ineffective attempt at sorting slice",
		Text: `
\'sort.Float64Slice\', \'sort.IntSlice\', and \'sort.StringSlice\' are
types, not functions. Doing \'x = sort.StringSlice(x)\' does nothing,
especially not sort any values. The correct usage is
\'sort.Sort(sort.StringSlice(x))\' or \'sort.StringSlice(x).Sort()\',
but there are more convenient helpers, namely \'sort.Float64s\',
\'sort.Ints\', and \'sort.Strings\'.
`,
		Since:    "2022.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4030": {
		Title: "Ineffective attempt at generating random number",
		Text: `
Functions in the \'math/rand\' package that accept upper limits, such
as \'Intn\', generate random numbers in the half-open interval [0,n). In
other words, the generated numbers will be \'>= 0\' and \'< n\' – they
don't include \'n\'. \'rand.Intn(1)\' therefore doesn't generate \'0\'
or \'1\', it always generates \'0\'.`,
		Since:    "2022.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA4031": {
		Title:    `Checking never-nil value against nil`,
		Since:    "2022.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5000": {
		Title:    `Assignment to nil map`,
		Since:    "2017.1",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5001": {
		Title:    `Deferring \'Close\' before checking for a possible error`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5002": {
		Title:    `The empty for loop (\"for {}\") spins and can block the scheduler`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5003": {
		Title: `Defers in infinite loops will never execute`,
		Text: `Defers are scoped to the surrounding function, not the surrounding
block. In a function that never returns, i.e. one containing an
infinite loop, defers will never execute.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5004": {
		Title:    `\"for { select { ...\" with an empty default branch spins`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5005": {
		Title: `The finalizer references the finalized object, preventing garbage collection`,
		Text: `A finalizer is a function associated with an object that runs when the
garbage collector is ready to collect said object, that is when the
object is no longer referenced by anything.

If the finalizer references the object, however, it will always remain
as the final reference to that object, preventing the garbage
collector from collecting the object. The finalizer will never run,
and the object will never be collected, leading to a memory leak. That
is why the finalizer should instead use its first argument to operate
on the object. That way, the number of references can temporarily go
to zero before the object is being passed to the finalizer.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5007": {
		Title: `Infinite recursive call`,
		Text: `A function that calls itself recursively needs to have an exit
condition. Otherwise it will recurse forever, until the system runs
out of memory.

This issue can be caused by simple bugs such as forgetting to add an
exit condition. It can also happen "on purpose". Some languages have
tail call optimization which makes certain infinite recursive calls
safe to use. Go, however, does not implement TCO, and as such a loop
should be used instead.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5008": {
		Title:    `Invalid struct tag`,
		Since:    "2019.2",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5009": {
		Title:    `Invalid Printf call`,
		Since:    "2019.2",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5010": {
		Title: `Impossible type assertion`,

		Text: `Some type assertions can be statically proven to be
impossible. This is the case when the method sets of both
arguments of the type assertion conflict with each other, for
example by containing the same method with different
signatures.

The Go compiler already applies this check when asserting from an
interface value to a concrete type. If the concrete type misses
methods from the interface, or if function signatures don't match,
then the type assertion can never succeed.

This check applies the same logic when asserting from one interface to
another. If both interface types contain the same method but with
different signatures, then the type assertion can never succeed,
either.`,

		Since:    "2020.1",
		Severity: lint.SeverityWarning,
		// Technically this should be MergeIfAll, but the Go compiler
		// already flags some impossible type assertions, so
		// MergeIfAny is consistent with the compiler.
		MergeIf: lint.MergeIfAny,
	},

	"SA5011": {
		Title: `Possible nil pointer dereference`,

		Text: `A pointer is being dereferenced unconditionally, while
also being checked against nil in another place. This suggests that
the pointer may be nil and dereferencing it may panic. This is
commonly a result of improperly ordered code or missing return
statements. Consider the following examples:

    func fn(x *int) {
        fmt.Println(*x)

        // This nil check is equally important for the previous dereference
        if x != nil {
            foo(*x)
        }
    }

    func TestFoo(t *testing.T) {
        x := compute()
        if x == nil {
            t.Errorf("nil pointer received")
        }

        // t.Errorf does not abort the test, so if x is nil, the next line will panic.
        foo(*x)
    }

Staticcheck tries to deduce which functions abort control flow.
For example, it is aware that a function will not continue
execution after a call to \'panic\' or \'log.Fatal\'. However, sometimes
this detection fails, in particular in the presence of
conditionals. Consider the following example:

    func Log(msg string, level int) {
        fmt.Println(msg)
        if level == levelFatal {
            os.Exit(1)
        }
    }

    func Fatal(msg string) {
        Log(msg, levelFatal)
    }

    func fn(x *int) {
        if x == nil {
            Fatal("unexpected nil pointer")
        }
        fmt.Println(*x)
    }

Staticcheck will flag the dereference of \'x\', even though it is perfectly
safe. Staticcheck is not able to deduce that a call to
Fatal will exit the program. For the time being, the easiest
workaround is to modify the definition of Fatal like so:

    func Fatal(msg string) {
        Log(msg, levelFatal)
        panic("unreachable")
    }

We also hard-code functions from common logging packages such as
logrus. Please file an issue if we're missing support for a
popular package.`,
		Since:    "2020.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA5012": {
		Title: "Passing odd-sized slice to function expecting even size",
		Text: `Some functions that take slices as parameters expect the slices to have an even number of elements. 
Often, these functions treat elements in a slice as pairs. 
For example, \'strings.NewReplacer\' takes pairs of old and new strings, 
and calling it with an odd number of elements would be an error.`,
		Since:    "2020.2",
		Severity: lint.SeverityError,
		MergeIf:  lint.MergeIfAny,
	},

	"SA6000": {
		Title:    `Using \'regexp.Match\' or related in a loop, should use \'regexp.Compile\'`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA6001": {
		Title: `Missing an optimization opportunity when indexing maps by byte slices`,

		Text: `Map keys must be comparable, which precludes the use of byte slices.
This usually leads to using string keys and converting byte slices to
strings.

Normally, a conversion of a byte slice to a string needs to copy the data and
causes allocations. The compiler, however, recognizes \'m[string(b)]\' and
uses the data of \'b\' directly, without copying it, because it knows that
the data can't change during the map lookup. This leads to the
counter-intuitive situation that

    k := string(b)
    println(m[k])
    println(m[k])

will be less efficient than

    println(m[string(b)])
    println(m[string(b)])

because the first version needs to copy and allocate, while the second
one does not.

For some history on this optimization, check out commit
f5f5a8b6209f84961687d993b93ea0d397f5d5bf in the Go repository.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA6002": {
		Title: `Storing non-pointer values in \'sync.Pool\' allocates memory`,
		Text: `A \'sync.Pool\' is used to avoid unnecessary allocations and reduce the
amount of work the garbage collector has to do.

When passing a value that is not a pointer to a function that accepts
an interface, the value needs to be placed on the heap, which means an
additional allocation. Slices are a common thing to put in sync.Pools,
and they're structs with 3 fields (length, capacity, and a pointer to
an array). In order to avoid the extra allocation, one should store a
pointer to the slice instead.

See the comments on https://go-review.googlesource.com/c/go/+/24371
that discuss this problem.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA6003": {
		Title: `Converting a string to a slice of runes before ranging over it`,
		Text: `You may want to loop over the runes in a string. Instead of converting
the string to a slice of runes and looping over that, you can loop
over the string itself. That is,

    for _, r := range s {}

and

    for _, r := range []rune(s) {}

will yield the same values. The first version, however, will be faster
and avoid unnecessary memory allocations.

Do note that if you are interested in the indices, ranging over a
string and over a slice of runes will yield different indices. The
first one yields byte offsets, while the second one yields indices in
the slice of runes.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA6005": {
		Title: `Inefficient string comparison with \'strings.ToLower\' or \'strings.ToUpper\'`,
		Text: `Converting two strings to the same case and comparing them like so

    if strings.ToLower(s1) == strings.ToLower(s2) {
        ...
    }

is significantly more expensive than comparing them with
\'strings.EqualFold(s1, s2)\'. This is due to memory usage as well as
computational complexity.

\'strings.ToLower\' will have to allocate memory for the new strings, as
well as convert both strings fully, even if they differ on the very
first byte. strings.EqualFold, on the other hand, compares the strings
one character at a time. It doesn't need to create two intermediate
strings and can return as soon as the first non-matching character has
been found.

For a more in-depth explanation of this issue, see
https://blog.digitalocean.com/how-to-efficiently-compare-strings-in-go/`,
		Since:    "2019.2",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA9001": {
		Title:    `Defers in range loops may not run when you expect them to`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA9002": {
		Title:    `Using a non-octal \'os.FileMode\' that looks like it was meant to be in octal.`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA9003": {
		Title:    `Empty body in an if or else branch`,
		Since:    "2017.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA9004": {
		Title: `Only the first constant has an explicit type`,

		Text: `In a constant declaration such as the following:

    const (
        First byte = 1
        Second     = 2
    )

the constant Second does not have the same type as the constant First.
This construct shouldn't be confused with

    const (
        First byte = iota
        Second
    )

where \'First\' and \'Second\' do indeed have the same type. The type is only
passed on when no explicit value is assigned to the constant.

When declaring enumerations with explicit values it is therefore
important not to write

    const (
          EnumFirst EnumType = 1
          EnumSecond         = 2
          EnumThird          = 3
    )

This discrepancy in types can cause various confusing behaviors and
bugs.


Wrong type in variable declarations

The most obvious issue with such incorrect enumerations expresses
itself as a compile error:

    package pkg

    const (
        EnumFirst  uint8 = 1
        EnumSecond       = 2
    )

    func fn(useFirst bool) {
        x := EnumSecond
        if useFirst {
            x = EnumFirst
        }
    }

fails to compile with

    ./const.go:11:5: cannot use EnumFirst (type uint8) as type int in assignment


Losing method sets

A more subtle issue occurs with types that have methods and optional
interfaces. Consider the following:

    package main

    import "fmt"

    type Enum int

    func (e Enum) String() string {
        return "an enum"
    }

    const (
        EnumFirst  Enum = 1
        EnumSecond      = 2
    )

    func main() {
        fmt.Println(EnumFirst)
        fmt.Println(EnumSecond)
    }

This code will output

    an enum
    2

as \'EnumSecond\' has no explicit type, and thus defaults to \'int\'.`,
		Since:    "2019.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA9005": {
		Title: `Trying to marshal a struct with no public fields nor custom marshaling`,
		Text: `
The \'encoding/json\' and \'encoding/xml\' packages only operate on exported
fields in structs, not unexported ones. It is usually an error to try
to (un)marshal structs that only consist of unexported fields.

This check will not flag calls involving types that define custom
marshaling behavior, e.g. via \'MarshalJSON\' methods. It will also not
flag empty structs.`,
		Since:    "2019.2",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAll,
	},

	"SA9006": {
		Title: `Dubious bit shifting of a fixed size integer value`,
		Text: `Bit shifting a value past its size will always clear the value.

For instance:

    v := int8(42)
    v >>= 8

will always result in 0.

This check flags bit shifting operations on fixed size integer values only.
That is, int, uint and uintptr are never flagged to avoid potential false
positives in somewhat exotic but valid bit twiddling tricks:

    // Clear any value above 32 bits if integers are more than 32 bits.
    func f(i int) int {
        v := i >> 32
        v = v << 32
        return i-v
    }`,
		Since:    "2020.2",
		Severity: lint.SeverityWarning,
		// Technically this should be MergeIfAll, because the type of
		// v might be different for different build tags. Practically,
		// don't write code that depends on that.
		MergeIf: lint.MergeIfAny,
	},

	"SA9007": {
		Title: "Deleting a directory that shouldn't be deleted",
		Text: `
It is virtually never correct to delete system directories such as
/tmp or the user's home directory. However, it can be fairly easy to
do by mistake, for example by mistakingly using \'os.TempDir\' instead
of \'ioutil.TempDir\', or by forgetting to add a suffix to the result
of \'os.UserHomeDir\'.

Writing

    d := os.TempDir()
    defer os.RemoveAll(d)

in your unit tests will have a devastating effect on the stability of your system.

This check flags attempts at deleting the following directories:

- os.TempDir
- os.UserCacheDir
- os.UserConfigDir
- os.UserHomeDir
`,
		Since:    "2022.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},

	"SA9008": {
		Title: `\'else\' branch of a type assertion is probably not reading the right value`,
		Text: `
When declaring variables as part of an \'if\' statement (like in \"if
foo := ...; foo {\"), the same variables will also be in the scope of
the \'else\' branch. This means that in the following example

    if x, ok := x.(int); ok {
        // ...
    } else {
        fmt.Println("unexpected type %T", x)
    }

\'x\' in the \'else\' branch will refer to the \'x\' from \'x, ok
:=\'; it will not refer to the \'x\' that is being type-asserted. The
result of a failed type assertion is the zero value of the type that
is being asserted to, so \'x\' in the else branch will always have the
value \'0\' and the type \'int\'.
`,
		Since:    "2022.1",
		Severity: lint.SeverityWarning,
		MergeIf:  lint.MergeIfAny,
	},
})
