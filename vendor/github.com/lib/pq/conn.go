package pq

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/lib/pq/oid"
	"github.com/lib/pq/scram"
)

// Common error types
var (
	ErrNotSupported              = errors.New("pq: Unsupported command")
	ErrInFailedTransaction       = errors.New("pq: Could not complete operation in a failed transaction")
	ErrSSLNotSupported           = errors.New("pq: SSL is not enabled on the server")
	ErrSSLKeyUnknownOwnership    = errors.New("pq: Could not get owner information for private key, may not be properly protected")
	ErrSSLKeyHasWorldPermissions = errors.New("pq: Private key has world access. Permissions should be u=rw,g=r (0640) if owned by root, or u=rw (0600), or less")

	ErrCouldNotDetectUsername = errors.New("pq: Could not detect default username. Please provide one explicitly")

	errUnexpectedReady = errors.New("unexpected ReadyForQuery")
	errNoRowsAffected  = errors.New("no RowsAffected available after the empty statement")
	errNoLastInsertID  = errors.New("no LastInsertId available after the empty statement")
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Driver = Driver{}
)

// Driver is the Postgres database driver.
type Driver struct{}

// Open opens a new connection to the database. name is a connection string.
// Most users should only use it through database/sql package from the standard
// library.
func (d Driver) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func init() {
	sql.Register("postgres", &Driver{})
}

type parameterStatus struct {
	// server version in the same format as server_version_num, or 0 if
	// unavailable
	serverVersion int

	// the current location based on the TimeZone value of the session, if
	// available
	currentLocation *time.Location
}

type transactionStatus byte

const (
	txnStatusIdle                transactionStatus = 'I'
	txnStatusIdleInTransaction   transactionStatus = 'T'
	txnStatusInFailedTransaction transactionStatus = 'E'
)

func (s transactionStatus) String() string {
	switch s {
	case txnStatusIdle:
		return "idle"
	case txnStatusIdleInTransaction:
		return "idle in transaction"
	case txnStatusInFailedTransaction:
		return "in a failed transaction"
	default:
		errorf("unknown transactionStatus %d", s)
	}

	panic("not reached")
}

// Dialer is the dialer interface. It can be used to obtain more control over
// how pq creates network connections.
type Dialer interface {
	Dial(network, address string) (net.Conn, error)
	DialTimeout(network, address string, timeout time.Duration) (net.Conn, error)
}

// DialerContext is the context-aware dialer interface.
type DialerContext interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type defaultDialer struct {
	d net.Dialer
}

func (d defaultDialer) Dial(network, address string) (net.Conn, error) {
	return d.d.Dial(network, address)
}
func (d defaultDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}
func (d defaultDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.d.DialContext(ctx, network, address)
}

type conn struct {
	c         net.Conn
	buf       *bufio.Reader
	namei     int
	scratch   [512]byte
	txnStatus transactionStatus
	txnFinish func()

	// Save connection arguments to use during CancelRequest.
	dialer Dialer
	opts   values

	// Cancellation key data for use with CancelRequest messages.
	processID int
	secretKey int

	parameterStatus parameterStatus

	saveMessageType   byte
	saveMessageBuffer []byte

	// If an error is set, this connection is bad and all public-facing
	// functions should return the appropriate error by calling get()
	// (ErrBadConn) or getForNext().
	err syncErr

	// If set, this connection should never use the binary format when
	// receiving query results from prepared statements.  Only provided for
	// debugging.
	disablePreparedBinaryResult bool

	// Whether to always send []byte parameters over as binary.  Enables single
	// round-trip mode for non-prepared Query calls.
	binaryParameters bool

	// If true this connection is in the middle of a COPY
	inCopy bool

	// If not nil, notices will be synchronously sent here
	noticeHandler func(*Error)

	// If not nil, notifications will be synchronously sent here
	notificationHandler func(*Notification)

	// GSSAPI context
	gss GSS
}

type syncErr struct {
	err error
	sync.Mutex
}

// Return ErrBadConn if connection is bad.
func (e *syncErr) get() error {
	e.Lock()
	defer e.Unlock()
	if e.err != nil {
		return driver.ErrBadConn
	}
	return nil
}

// Return the error set on the connection. Currently only used by rows.Next.
func (e *syncErr) getForNext() error {
	e.Lock()
	defer e.Unlock()
	return e.err
}

// Set error, only if it isn't set yet.
func (e *syncErr) set(err error) {
	if err == nil {
		panic("attempt to set nil err")
	}
	e.Lock()
	defer e.Unlock()
	if e.err == nil {
		e.err = err
	}
}

// Handle driver-side settings in parsed connection string.
func (cn *conn) handleDriverSettings(o values) (err error) {
	boolSetting := func(key string, val *bool) error {
		if value, ok := o[key]; ok {
			if value == "yes" {
				*val = true
			} else if value == "no" {
				*val = false
			} else {
				return fmt.Errorf("unrecognized value %q for %s", value, key)
			}
		}
		return nil
	}

	err = boolSetting("disable_prepared_binary_result", &cn.disablePreparedBinaryResult)
	if err != nil {
		return err
	}
	return boolSetting("binary_parameters", &cn.binaryParameters)
}

func (cn *conn) handlePgpass(o values) {
	// if a password was supplied, do not process .pgpass
	if _, ok := o["password"]; ok {
		return
	}
	filename := os.Getenv("PGPASSFILE")
	if filename == "" {
		// XXX this code doesn't work on Windows where the default filename is
		// XXX %APPDATA%\postgresql\pgpass.conf
		// Prefer $HOME over user.Current due to glibc bug: golang.org/issue/13470
		userHome := os.Getenv("HOME")
		if userHome == "" {
			user, err := user.Current()
			if err != nil {
				return
			}
			userHome = user.HomeDir
		}
		filename = filepath.Join(userHome, ".pgpass")
	}
	fileinfo, err := os.Stat(filename)
	if err != nil {
		return
	}
	mode := fileinfo.Mode()
	if mode&(0x77) != 0 {
		// XXX should warn about incorrect .pgpass permissions as psql does
		return
	}
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(io.Reader(file))
	hostname := o["host"]
	ntw, _ := network(o)
	port := o["port"]
	db := o["dbname"]
	username := o["user"]
	// From: https://github.com/tg/pgpass/blob/master/reader.go
	getFields := func(s string) []string {
		fs := make([]string, 0, 5)
		f := make([]rune, 0, len(s))

		var esc bool
		for _, c := range s {
			switch {
			case esc:
				f = append(f, c)
				esc = false
			case c == '\\':
				esc = true
			case c == ':':
				fs = append(fs, string(f))
				f = f[:0]
			default:
				f = append(f, c)
			}
		}
		return append(fs, string(f))
	}
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		split := getFields(line)
		if len(split) != 5 {
			continue
		}
		if (split[0] == "*" || split[0] == hostname || (split[0] == "localhost" && (hostname == "" || ntw == "unix"))) && (split[1] == "*" || split[1] == port) && (split[2] == "*" || split[2] == db) && (split[3] == "*" || split[3] == username) {
			o["password"] = split[4]
			return
		}
	}
}

func (cn *conn) writeBuf(b byte) *writeBuf {
	cn.scratch[0] = b
	return &writeBuf{
		buf: cn.scratch[:5],
		pos: 1,
	}
}

// Open opens a new connection to the database. dsn is a connection string.
// Most users should only use it through database/sql package from the standard
// library.
func Open(dsn string) (_ driver.Conn, err error) {
	return DialOpen(defaultDialer{}, dsn)
}

// DialOpen opens a new connection to the database using a dialer.
func DialOpen(d Dialer, dsn string) (_ driver.Conn, err error) {
	c, err := NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	c.Dialer(d)
	return c.open(context.Background())
}

func (c *Connector) open(ctx context.Context) (cn *conn, err error) {
	// Handle any panics during connection initialization.  Note that we
	// specifically do *not* want to use errRecover(), as that would turn any
	// connection errors into ErrBadConns, hiding the real error message from
	// the user.
	defer errRecoverNoErrBadConn(&err)

	// Create a new values map (copy). This makes it so maps in different
	// connections do not reference the same underlying data structure, so it
	// is safe for multiple connections to concurrently write to their opts.
	o := make(values)
	for k, v := range c.opts {
		o[k] = v
	}

	cn = &conn{
		opts:   o,
		dialer: c.dialer,
	}
	err = cn.handleDriverSettings(o)
	if err != nil {
		return nil, err
	}
	cn.handlePgpass(o)

	cn.c, err = dial(ctx, c.dialer, o)
	if err != nil {
		return nil, err
	}

	err = cn.ssl(o)
	if err != nil {
		if cn.c != nil {
			cn.c.Close()
		}
		return nil, err
	}

	// cn.startup panics on error. Make sure we don't leak cn.c.
	panicking := true
	defer func() {
		if panicking {
			cn.c.Close()
		}
	}()

	cn.buf = bufio.NewReader(cn.c)
	cn.startup(o)

	// reset the deadline, in case one was set (see dial)
	if timeout, ok := o["connect_timeout"]; ok && timeout != "0" {
		err = cn.c.SetDeadline(time.Time{})
	}
	panicking = false
	return cn, err
}

func dial(ctx context.Context, d Dialer, o values) (net.Conn, error) {
	network, address := network(o)

	// Zero or not specified means wait indefinitely.
	if timeout, ok := o["connect_timeout"]; ok && timeout != "0" {
		seconds, err := strconv.ParseInt(timeout, 10, 0)
		if err != nil {
			return nil, fmt.Errorf("invalid value for parameter connect_timeout: %s", err)
		}
		duration := time.Duration(seconds) * time.Second

		// connect_timeout should apply to the entire connection establishment
		// procedure, so we both use a timeout for the TCP connection
		// establishment and set a deadline for doing the initial handshake.
		// The deadline is then reset after startup() is done.
		deadline := time.Now().Add(duration)
		var conn net.Conn
		if dctx, ok := d.(DialerContext); ok {
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			conn, err = dctx.DialContext(ctx, network, address)
		} else {
			conn, err = d.DialTimeout(network, address, duration)
		}
		if err != nil {
			return nil, err
		}
		err = conn.SetDeadline(deadline)
		return conn, err
	}
	if dctx, ok := d.(DialerContext); ok {
		return dctx.DialContext(ctx, network, address)
	}
	return d.Dial(network, address)
}

func network(o values) (string, string) {
	host := o["host"]

	if strings.HasPrefix(host, "/") {
		sockPath := path.Join(host, ".s.PGSQL."+o["port"])
		return "unix", sockPath
	}

	return "tcp", net.JoinHostPort(host, o["port"])
}

type values map[string]string

// scanner implements a tokenizer for libpq-style option strings.
type scanner struct {
	s []rune
	i int
}

// newScanner returns a new scanner initialized with the option string s.
func newScanner(s string) *scanner {
	return &scanner{[]rune(s), 0}
}

// Next returns the next rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}

// parseOpts parses the options from name and adds them to the values.
//
// The parsing code is based on conninfo_parse from libpq's fe-connect.c
func parseOpts(name string, o values) error {
	s := newScanner(name)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			o[string(keyRunes)] = ""
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return fmt.Errorf(`missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return fmt.Errorf(`unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		o[string(keyRunes)] = string(valRunes)
	}

	return nil
}

func (cn *conn) isInTransaction() bool {
	return cn.txnStatus == txnStatusIdleInTransaction ||
		cn.txnStatus == txnStatusInFailedTransaction
}

func (cn *conn) checkIsInTransaction(intxn bool) {
	if cn.isInTransaction() != intxn {
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected transaction status %v", cn.txnStatus)
	}
}

func (cn *conn) Begin() (_ driver.Tx, err error) {
	return cn.begin("")
}

func (cn *conn) begin(mode string) (_ driver.Tx, err error) {
	if err := cn.err.get(); err != nil {
		return nil, err
	}
	defer cn.errRecover(&err)

	cn.checkIsInTransaction(false)
	_, commandTag, err := cn.simpleExec("BEGIN" + mode)
	if err != nil {
		return nil, err
	}
	if commandTag != "BEGIN" {
		cn.err.set(driver.ErrBadConn)
		return nil, fmt.Errorf("unexpected command tag %s", commandTag)
	}
	if cn.txnStatus != txnStatusIdleInTransaction {
		cn.err.set(driver.ErrBadConn)
		return nil, fmt.Errorf("unexpected transaction status %v", cn.txnStatus)
	}
	return cn, nil
}

func (cn *conn) closeTxn() {
	if finish := cn.txnFinish; finish != nil {
		finish()
	}
}

func (cn *conn) Commit() (err error) {
	defer cn.closeTxn()
	if err := cn.err.get(); err != nil {
		return err
	}
	defer cn.errRecover(&err)

	cn.checkIsInTransaction(true)
	// We don't want the client to think that everything is okay if it tries
	// to commit a failed transaction.  However, no matter what we return,
	// database/sql will release this connection back into the free connection
	// pool so we have to abort the current transaction here.  Note that you
	// would get the same behaviour if you issued a COMMIT in a failed
	// transaction, so it's also the least surprising thing to do here.
	if cn.txnStatus == txnStatusInFailedTransaction {
		if err := cn.rollback(); err != nil {
			return err
		}
		return ErrInFailedTransaction
	}

	_, commandTag, err := cn.simpleExec("COMMIT")
	if err != nil {
		if cn.isInTransaction() {
			cn.err.set(driver.ErrBadConn)
		}
		return err
	}
	if commandTag != "COMMIT" {
		cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("unexpected command tag %s", commandTag)
	}
	cn.checkIsInTransaction(false)
	return nil
}

func (cn *conn) Rollback() (err error) {
	defer cn.closeTxn()
	if err := cn.err.get(); err != nil {
		return err
	}
	defer cn.errRecover(&err)
	return cn.rollback()
}

func (cn *conn) rollback() (err error) {
	cn.checkIsInTransaction(true)
	_, commandTag, err := cn.simpleExec("ROLLBACK")
	if err != nil {
		if cn.isInTransaction() {
			cn.err.set(driver.ErrBadConn)
		}
		return err
	}
	if commandTag != "ROLLBACK" {
		return fmt.Errorf("unexpected command tag %s", commandTag)
	}
	cn.checkIsInTransaction(false)
	return nil
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) simpleExec(q string) (res driver.Result, commandTag string, err error) {
	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			res, commandTag = cn.parseComplete(r.string())
		case 'Z':
			cn.processReadyForQuery(r)
			if res == nil && err == nil {
				err = errUnexpectedReady
			}
			// done
			return
		case 'E':
			err = parseError(r)
		case 'I':
			res = emptyRows
		case 'T', 'D':
			// ignore any results
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unknown response for simple query: %q", t)
		}
	}
}

func (cn *conn) simpleQuery(q string) (res *rows, err error) {
	defer cn.errRecover(&err)

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'C', 'I':
			// We allow queries which don't return any results through Query as
			// well as Exec.  We still have to give database/sql a rows object
			// the user can close, though, to avoid connections from being
			// leaked.  A "rows" with done=true works fine for that purpose.
			if err != nil {
				cn.err.set(driver.ErrBadConn)
				errorf("unexpected message %q in simple query execution", t)
			}
			if res == nil {
				res = &rows{
					cn: cn,
				}
			}
			// Set the result and tag to the last command complete if there wasn't a
			// query already run. Although queries usually return from here and cede
			// control to Next, a query with zero results does not.
			if t == 'C' {
				res.result, res.tag = cn.parseComplete(r.string())
				if res.colNames != nil {
					return
				}
			}
			res.done = true
		case 'Z':
			cn.processReadyForQuery(r)
			// done
			return
		case 'E':
			res = nil
			err = parseError(r)
		case 'D':
			if res == nil {
				cn.err.set(driver.ErrBadConn)
				errorf("unexpected DataRow in simple query execution")
			}
			// the query didn't fail; kick off to Next
			cn.saveMessage(t, r)
			return
		case 'T':
			// res might be non-nil here if we received a previous
			// CommandComplete, but that's fine; just overwrite it
			res = &rows{cn: cn}
			res.rowsHeader = parsePortalRowDescribe(r)

			// To work around a bug in QueryRow in Go 1.2 and earlier, wait
			// until the first DataRow has been received.
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unknown response for simple query: %q", t)
		}
	}
}

type noRows struct{}

var emptyRows noRows

var _ driver.Result = noRows{}

func (noRows) LastInsertId() (int64, error) {
	return 0, errNoLastInsertID
}

func (noRows) RowsAffected() (int64, error) {
	return 0, errNoRowsAffected
}

// Decides which column formats to use for a prepared statement.  The input is
// an array of type oids, one element per result column.
func decideColumnFormats(colTyps []fieldDesc, forceText bool) (colFmts []format, colFmtData []byte) {
	if len(colTyps) == 0 {
		return nil, colFmtDataAllText
	}

	colFmts = make([]format, len(colTyps))
	if forceText {
		return colFmts, colFmtDataAllText
	}

	allBinary := true
	allText := true
	for i, t := range colTyps {
		switch t.OID {
		// This is the list of types to use binary mode for when receiving them
		// through a prepared statement.  If a type appears in this list, it
		// must also be implemented in binaryDecode in encode.go.
		case oid.T_bytea:
			fallthrough
		case oid.T_int8:
			fallthrough
		case oid.T_int4:
			fallthrough
		case oid.T_int2:
			fallthrough
		case oid.T_uuid:
			colFmts[i] = formatBinary
			allText = false

		default:
			allBinary = false
		}
	}

	if allBinary {
		return colFmts, colFmtDataAllBinary
	} else if allText {
		return colFmts, colFmtDataAllText
	} else {
		colFmtData = make([]byte, 2+len(colFmts)*2)
		binary.BigEndian.PutUint16(colFmtData, uint16(len(colFmts)))
		for i, v := range colFmts {
			binary.BigEndian.PutUint16(colFmtData[2+i*2:], uint16(v))
		}
		return colFmts, colFmtData
	}
}

func (cn *conn) prepareTo(q, stmtName string) *stmt {
	st := &stmt{cn: cn, name: stmtName}

	b := cn.writeBuf('P')
	b.string(st.name)
	b.string(q)
	b.int16(0)

	b.next('D')
	b.byte('S')
	b.string(st.name)

	b.next('S')
	cn.send(b)

	cn.readParseResponse()
	st.paramTyps, st.colNames, st.colTyps = cn.readStatementDescribeResponse()
	st.colFmts, st.colFmtData = decideColumnFormats(st.colTyps, cn.disablePreparedBinaryResult)
	cn.readReadyForQuery()
	return st
}

func (cn *conn) Prepare(q string) (_ driver.Stmt, err error) {
	if err := cn.err.get(); err != nil {
		return nil, err
	}
	defer cn.errRecover(&err)

	if len(q) >= 4 && strings.EqualFold(q[:4], "COPY") {
		s, err := cn.prepareCopyIn(q)
		if err == nil {
			cn.inCopy = true
		}
		return s, err
	}
	return cn.prepareTo(q, cn.gname()), nil
}

func (cn *conn) Close() (err error) {
	// Skip cn.bad return here because we always want to close a connection.
	defer cn.errRecover(&err)

	// Ensure that cn.c.Close is always run. Since error handling is done with
	// panics and cn.errRecover, the Close must be in a defer.
	defer func() {
		cerr := cn.c.Close()
		if err == nil {
			err = cerr
		}
	}()

	// Don't go through send(); ListenerConn relies on us not scribbling on the
	// scratch buffer of this connection.
	return cn.sendSimpleMessage('X')
}

// Implement the "Queryer" interface
func (cn *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return cn.query(query, args)
}

func (cn *conn) query(query string, args []driver.Value) (_ *rows, err error) {
	if err := cn.err.get(); err != nil {
		return nil, err
	}
	if cn.inCopy {
		return nil, errCopyInProgress
	}
	defer cn.errRecover(&err)

	// Check to see if we can use the "simpleQuery" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		return cn.simpleQuery(query)
	}

	if cn.binaryParameters {
		cn.sendBinaryModeQuery(query, args)

		cn.readParseResponse()
		cn.readBindResponse()
		rows := &rows{cn: cn}
		rows.rowsHeader = cn.readPortalDescribeResponse()
		cn.postExecuteWorkaround()
		return rows, nil
	}
	st := cn.prepareTo(query, "")
	st.exec(args)
	return &rows{
		cn:         cn,
		rowsHeader: st.rowsHeader,
	}, nil
}

// Implement the optional "Execer" interface for one-shot queries
func (cn *conn) Exec(query string, args []driver.Value) (res driver.Result, err error) {
	if err := cn.err.get(); err != nil {
		return nil, err
	}
	defer cn.errRecover(&err)

	// Check to see if we can use the "simpleExec" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		// ignore commandTag, our caller doesn't care
		r, _, err := cn.simpleExec(query)
		return r, err
	}

	if cn.binaryParameters {
		cn.sendBinaryModeQuery(query, args)

		cn.readParseResponse()
		cn.readBindResponse()
		cn.readPortalDescribeResponse()
		cn.postExecuteWorkaround()
		res, _, err = cn.readExecuteResponse("Execute")
		return res, err
	}
	// Use the unnamed statement to defer planning until bind
	// time, or else value-based selectivity estimates cannot be
	// used.
	st := cn.prepareTo(query, "")
	r, err := st.Exec(args)
	if err != nil {
		panic(err)
	}
	return r, err
}

type safeRetryError struct {
	Err error
}

func (se *safeRetryError) Error() string {
	return se.Err.Error()
}

func (cn *conn) send(m *writeBuf) {
	n, err := cn.c.Write(m.wrap())
	if err != nil {
		if n == 0 {
			err = &safeRetryError{Err: err}
		}
		panic(err)
	}
}

func (cn *conn) sendStartupPacket(m *writeBuf) error {
	_, err := cn.c.Write((m.wrap())[1:])
	return err
}

// Send a message of type typ to the server on the other end of cn.  The
// message should have no payload.  This method does not use the scratch
// buffer.
func (cn *conn) sendSimpleMessage(typ byte) (err error) {
	_, err = cn.c.Write([]byte{typ, '\x00', '\x00', '\x00', '\x04'})
	return err
}

// saveMessage memorizes a message and its buffer in the conn struct.
// recvMessage will then return these values on the next call to it.  This
// method is useful in cases where you have to see what the next message is
// going to be (e.g. to see whether it's an error or not) but you can't handle
// the message yourself.
func (cn *conn) saveMessage(typ byte, buf *readBuf) {
	if cn.saveMessageType != 0 {
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected saveMessageType %d", cn.saveMessageType)
	}
	cn.saveMessageType = typ
	cn.saveMessageBuffer = *buf
}

// recvMessage receives any message from the backend, or returns an error if
// a problem occurred while reading the message.
func (cn *conn) recvMessage(r *readBuf) (byte, error) {
	// workaround for a QueryRow bug, see exec
	if cn.saveMessageType != 0 {
		t := cn.saveMessageType
		*r = cn.saveMessageBuffer
		cn.saveMessageType = 0
		cn.saveMessageBuffer = nil
		return t, nil
	}

	x := cn.scratch[:5]
	_, err := io.ReadFull(cn.buf, x)
	if err != nil {
		return 0, err
	}

	// read the type and length of the message that follows
	t := x[0]
	n := int(binary.BigEndian.Uint32(x[1:])) - 4
	var y []byte
	if n <= len(cn.scratch) {
		y = cn.scratch[:n]
	} else {
		y = make([]byte, n)
	}
	_, err = io.ReadFull(cn.buf, y)
	if err != nil {
		return 0, err
	}
	*r = y
	return t, nil
}

// recv receives a message from the backend, but if an error happened while
// reading the message or the received message was an ErrorResponse, it panics.
// NoticeResponses are ignored.  This function should generally be used only
// during the startup sequence.
func (cn *conn) recv() (t byte, r *readBuf) {
	for {
		var err error
		r = &readBuf{}
		t, err = cn.recvMessage(r)
		if err != nil {
			panic(err)
		}
		switch t {
		case 'E':
			panic(parseError(r))
		case 'N':
			if n := cn.noticeHandler; n != nil {
				n(parseError(r))
			}
		case 'A':
			if n := cn.notificationHandler; n != nil {
				n(recvNotification(r))
			}
		default:
			return
		}
	}
}

// recv1Buf is exactly equivalent to recv1, except it uses a buffer supplied by
// the caller to avoid an allocation.
func (cn *conn) recv1Buf(r *readBuf) byte {
	for {
		t, err := cn.recvMessage(r)
		if err != nil {
			panic(err)
		}

		switch t {
		case 'A':
			if n := cn.notificationHandler; n != nil {
				n(recvNotification(r))
			}
		case 'N':
			if n := cn.noticeHandler; n != nil {
				n(parseError(r))
			}
		case 'S':
			cn.processParameterStatus(r)
		default:
			return t
		}
	}
}

// recv1 receives a message from the backend, panicking if an error occurs
// while attempting to read it.  All asynchronous messages are ignored, with
// the exception of ErrorResponse.
func (cn *conn) recv1() (t byte, r *readBuf) {
	r = &readBuf{}
	t = cn.recv1Buf(r)
	return t, r
}

func (cn *conn) ssl(o values) error {
	upgrade, err := ssl(o)
	if err != nil {
		return err
	}

	if upgrade == nil {
		// Nothing to do
		return nil
	}

	w := cn.writeBuf(0)
	w.int32(80877103)
	if err = cn.sendStartupPacket(w); err != nil {
		return err
	}

	b := cn.scratch[:1]
	_, err = io.ReadFull(cn.c, b)
	if err != nil {
		return err
	}

	if b[0] != 'S' {
		return ErrSSLNotSupported
	}

	cn.c, err = upgrade(cn.c)
	return err
}

// isDriverSetting returns true iff a setting is purely for configuring the
// driver's options and should not be sent to the server in the connection
// startup packet.
func isDriverSetting(key string) bool {
	switch key {
	case "host", "port":
		return true
	case "password":
		return true
	case "sslmode", "sslcert", "sslkey", "sslrootcert", "sslinline", "sslsni":
		return true
	case "fallback_application_name":
		return true
	case "connect_timeout":
		return true
	case "disable_prepared_binary_result":
		return true
	case "binary_parameters":
		return true
	case "krbsrvname":
		return true
	case "krbspn":
		return true
	default:
		return false
	}
}

func (cn *conn) startup(o values) {
	w := cn.writeBuf(0)
	w.int32(196608)
	// Send the backend the name of the database we want to connect to, and the
	// user we want to connect as.  Additionally, we send over any run-time
	// parameters potentially included in the connection string.  If the server
	// doesn't recognize any of them, it will reply with an error.
	for k, v := range o {
		if isDriverSetting(k) {
			// skip options which can't be run-time parameters
			continue
		}
		// The protocol requires us to supply the database name as "database"
		// instead of "dbname".
		if k == "dbname" {
			k = "database"
		}
		w.string(k)
		w.string(v)
	}
	w.string("")
	if err := cn.sendStartupPacket(w); err != nil {
		panic(err)
	}

	for {
		t, r := cn.recv()
		switch t {
		case 'K':
			cn.processBackendKeyData(r)
		case 'S':
			cn.processParameterStatus(r)
		case 'R':
			cn.auth(r, o)
		case 'Z':
			cn.processReadyForQuery(r)
			return
		default:
			errorf("unknown response for startup: %q", t)
		}
	}
}

func (cn *conn) auth(r *readBuf, o values) {
	switch code := r.int32(); code {
	case 0:
		// OK
	case 3:
		w := cn.writeBuf('p')
		w.string(o["password"])
		cn.send(w)

		t, r := cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	case 5:
		s := string(r.next(4))
		w := cn.writeBuf('p')
		w.string("md5" + md5s(md5s(o["password"]+o["user"])+s))
		cn.send(w)

		t, r := cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	case 7: // GSSAPI, startup
		if newGss == nil {
			errorf("kerberos error: no GSSAPI provider registered (import github.com/lib/pq/auth/kerberos if you need Kerberos support)")
		}
		cli, err := newGss()
		if err != nil {
			errorf("kerberos error: %s", err.Error())
		}

		var token []byte

		if spn, ok := o["krbspn"]; ok {
			// Use the supplied SPN if provided..
			token, err = cli.GetInitTokenFromSpn(spn)
		} else {
			// Allow the kerberos service name to be overridden
			service := "postgres"
			if val, ok := o["krbsrvname"]; ok {
				service = val
			}

			token, err = cli.GetInitToken(o["host"], service)
		}

		if err != nil {
			errorf("failed to get Kerberos ticket: %q", err)
		}

		w := cn.writeBuf('p')
		w.bytes(token)
		cn.send(w)

		// Store for GSSAPI continue message
		cn.gss = cli

	case 8: // GSSAPI continue

		if cn.gss == nil {
			errorf("GSSAPI protocol error")
		}

		b := []byte(*r)

		done, tokOut, err := cn.gss.Continue(b)
		if err == nil && !done {
			w := cn.writeBuf('p')
			w.bytes(tokOut)
			cn.send(w)
		}

		// Errors fall through and read the more detailed message
		// from the server..

	case 10:
		sc := scram.NewClient(sha256.New, o["user"], o["password"])
		sc.Step(nil)
		if sc.Err() != nil {
			errorf("SCRAM-SHA-256 error: %s", sc.Err().Error())
		}
		scOut := sc.Out()

		w := cn.writeBuf('p')
		w.string("SCRAM-SHA-256")
		w.int32(len(scOut))
		w.bytes(scOut)
		cn.send(w)

		t, r := cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 11 {
			errorf("unexpected authentication response: %q", t)
		}

		nextStep := r.next(len(*r))
		sc.Step(nextStep)
		if sc.Err() != nil {
			errorf("SCRAM-SHA-256 error: %s", sc.Err().Error())
		}

		scOut = sc.Out()
		w = cn.writeBuf('p')
		w.bytes(scOut)
		cn.send(w)

		t, r = cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 12 {
			errorf("unexpected authentication response: %q", t)
		}

		nextStep = r.next(len(*r))
		sc.Step(nextStep)
		if sc.Err() != nil {
			errorf("SCRAM-SHA-256 error: %s", sc.Err().Error())
		}

	default:
		errorf("unknown authentication response: %d", code)
	}
}

type format int

const formatText format = 0
const formatBinary format = 1

// One result-column format code with the value 1 (i.e. all binary).
var colFmtDataAllBinary = []byte{0, 1, 0, 1}

// No result-column format codes (i.e. all text).
var colFmtDataAllText = []byte{0, 0}

type stmt struct {
	cn   *conn
	name string
	rowsHeader
	colFmtData []byte
	paramTyps  []oid.Oid
	closed     bool
}

func (st *stmt) Close() (err error) {
	if st.closed {
		return nil
	}
	if err := st.cn.err.get(); err != nil {
		return err
	}
	defer st.cn.errRecover(&err)

	w := st.cn.writeBuf('C')
	w.byte('S')
	w.string(st.name)
	st.cn.send(w)

	st.cn.send(st.cn.writeBuf('S'))

	t, _ := st.cn.recv1()
	if t != '3' {
		st.cn.err.set(driver.ErrBadConn)
		errorf("unexpected close response: %q", t)
	}
	st.closed = true

	t, r := st.cn.recv1()
	if t != 'Z' {
		st.cn.err.set(driver.ErrBadConn)
		errorf("expected ready for query, but got: %q", t)
	}
	st.cn.processReadyForQuery(r)

	return nil
}

func (st *stmt) Query(v []driver.Value) (r driver.Rows, err error) {
	return st.query(v)
}

func (st *stmt) query(v []driver.Value) (r *rows, err error) {
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}
	defer st.cn.errRecover(&err)

	st.exec(v)
	return &rows{
		cn:         st.cn,
		rowsHeader: st.rowsHeader,
	}, nil
}

func (st *stmt) Exec(v []driver.Value) (res driver.Result, err error) {
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}
	defer st.cn.errRecover(&err)

	st.exec(v)
	res, _, err = st.cn.readExecuteResponse("simple query")
	return res, err
}

func (st *stmt) exec(v []driver.Value) {
	if len(v) >= 65536 {
		errorf("got %d parameters but PostgreSQL only supports 65535 parameters", len(v))
	}
	if len(v) != len(st.paramTyps) {
		errorf("got %d parameters but the statement requires %d", len(v), len(st.paramTyps))
	}

	cn := st.cn
	w := cn.writeBuf('B')
	w.byte(0) // unnamed portal
	w.string(st.name)

	if cn.binaryParameters {
		cn.sendBinaryParameters(w, v)
	} else {
		w.int16(0)
		w.int16(len(v))
		for i, x := range v {
			if x == nil {
				w.int32(-1)
			} else {
				b := encode(&cn.parameterStatus, x, st.paramTyps[i])
				w.int32(len(b))
				w.bytes(b)
			}
		}
	}
	w.bytes(st.colFmtData)

	w.next('E')
	w.byte(0)
	w.int32(0)

	w.next('S')
	cn.send(w)

	cn.readBindResponse()
	cn.postExecuteWorkaround()

}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}

// parseComplete parses the "command tag" from a CommandComplete message, and
// returns the number of rows affected (if applicable) and a string
// identifying only the command that was executed, e.g. "ALTER TABLE".  If the
// command tag could not be parsed, parseComplete panics.
func (cn *conn) parseComplete(commandTag string) (driver.Result, string) {
	commandsWithAffectedRows := []string{
		"SELECT ",
		// INSERT is handled below
		"UPDATE ",
		"DELETE ",
		"FETCH ",
		"MOVE ",
		"COPY ",
	}

	var affectedRows *string
	for _, tag := range commandsWithAffectedRows {
		if strings.HasPrefix(commandTag, tag) {
			t := commandTag[len(tag):]
			affectedRows = &t
			commandTag = tag[:len(tag)-1]
			break
		}
	}
	// INSERT also includes the oid of the inserted row in its command tag.
	// Oids in user tables are deprecated, and the oid is only returned when
	// exactly one row is inserted, so it's unlikely to be of value to any
	// real-world application and we can ignore it.
	if affectedRows == nil && strings.HasPrefix(commandTag, "INSERT ") {
		parts := strings.Split(commandTag, " ")
		if len(parts) != 3 {
			cn.err.set(driver.ErrBadConn)
			errorf("unexpected INSERT command tag %s", commandTag)
		}
		affectedRows = &parts[len(parts)-1]
		commandTag = "INSERT"
	}
	// There should be no affected rows attached to the tag, just return it
	if affectedRows == nil {
		return driver.RowsAffected(0), commandTag
	}
	n, err := strconv.ParseInt(*affectedRows, 10, 64)
	if err != nil {
		cn.err.set(driver.ErrBadConn)
		errorf("could not parse commandTag: %s", err)
	}
	return driver.RowsAffected(n), commandTag
}

type rowsHeader struct {
	colNames []string
	colTyps  []fieldDesc
	colFmts  []format
}

type rows struct {
	cn     *conn
	finish func()
	rowsHeader
	done   bool
	rb     readBuf
	result driver.Result
	tag    string

	next *rowsHeader
}

func (rs *rows) Close() error {
	if finish := rs.finish; finish != nil {
		defer finish()
	}
	// no need to look at cn.bad as Next() will
	for {
		err := rs.Next(nil)
		switch err {
		case nil:
		case io.EOF:
			// rs.Next can return io.EOF on both 'Z' (ready for query) and 'T' (row
			// description, used with HasNextResultSet). We need to fetch messages until
			// we hit a 'Z', which is done by waiting for done to be set.
			if rs.done {
				return nil
			}
		default:
			return err
		}
	}
}

func (rs *rows) Columns() []string {
	return rs.colNames
}

func (rs *rows) Result() driver.Result {
	if rs.result == nil {
		return emptyRows
	}
	return rs.result
}

func (rs *rows) Tag() string {
	return rs.tag
}

func (rs *rows) Next(dest []driver.Value) (err error) {
	if rs.done {
		return io.EOF
	}

	conn := rs.cn
	if err := conn.err.getForNext(); err != nil {
		return err
	}
	defer conn.errRecover(&err)

	for {
		t := conn.recv1Buf(&rs.rb)
		switch t {
		case 'E':
			err = parseError(&rs.rb)
		case 'C', 'I':
			if t == 'C' {
				rs.result, rs.tag = conn.parseComplete(rs.rb.string())
			}
			continue
		case 'Z':
			conn.processReadyForQuery(&rs.rb)
			rs.done = true
			if err != nil {
				return err
			}
			return io.EOF
		case 'D':
			n := rs.rb.int16()
			if err != nil {
				conn.err.set(driver.ErrBadConn)
				errorf("unexpected DataRow after error %s", err)
			}
			if n < len(dest) {
				dest = dest[:n]
			}
			for i := range dest {
				l := rs.rb.int32()
				if l == -1 {
					dest[i] = nil
					continue
				}
				dest[i] = decode(&conn.parameterStatus, rs.rb.next(l), rs.colTyps[i].OID, rs.colFmts[i])
			}
			return
		case 'T':
			next := parsePortalRowDescribe(&rs.rb)
			rs.next = &next
			return io.EOF
		default:
			errorf("unexpected message after execute: %q", t)
		}
	}
}

func (rs *rows) HasNextResultSet() bool {
	hasNext := rs.next != nil && !rs.done
	return hasNext
}

func (rs *rows) NextResultSet() error {
	if rs.next == nil {
		return io.EOF
	}
	rs.rowsHeader = *rs.next
	rs.next = nil
	return nil
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//    tblname := "my_table"
//    data := "my_data"
//    quoted := pq.QuoteIdentifier(tblname)
//    err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}

// QuoteLiteral quotes a 'literal' (e.g. a parameter, often used to pass literal
// to DDL and other statements that do not accept parameters) to be used as part
// of an SQL statement.  For example:
//
//    exp_date := pq.QuoteLiteral("2023-01-05 15:00:00Z")
//    err := db.Exec(fmt.Sprintf("CREATE ROLE my_user VALID UNTIL %s", exp_date))
//
// Any single quotes in name will be escaped. Any backslashes (i.e. "\") will be
// replaced by two backslashes (i.e. "\\") and the C-style escape identifier
// that PostgreSQL provides ('E') will be prepended to the string.
func QuoteLiteral(literal string) string {
	// This follows the PostgreSQL internal algorithm for handling quoted literals
	// from libpq, which can be found in the "PQEscapeStringInternal" function,
	// which is found in the libpq/fe-exec.c source file:
	// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/interfaces/libpq/fe-exec.c
	//
	// substitute any single-quotes (') with two single-quotes ('')
	literal = strings.Replace(literal, `'`, `''`, -1)
	// determine if the string has any backslashes (\) in it.
	// if it does, replace any backslashes (\) with two backslashes (\\)
	// then, we need to wrap the entire string with a PostgreSQL
	// C-style escape. Per how "PQEscapeStringInternal" handles this case, we
	// also add a space before the "E"
	if strings.Contains(literal, `\`) {
		literal = strings.Replace(literal, `\`, `\\`, -1)
		literal = ` E'` + literal + `'`
	} else {
		// otherwise, we can just wrap the literal with a pair of single quotes
		literal = `'` + literal + `'`
	}
	return literal
}

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (cn *conn) sendBinaryParameters(b *writeBuf, args []driver.Value) {
	// Do one pass over the parameters to see if we're going to send any of
	// them over in binary.  If we are, create a paramFormats array at the
	// same time.
	var paramFormats []int
	for i, x := range args {
		_, ok := x.([]byte)
		if ok {
			if paramFormats == nil {
				paramFormats = make([]int, len(args))
			}
			paramFormats[i] = 1
		}
	}
	if paramFormats == nil {
		b.int16(0)
	} else {
		b.int16(len(paramFormats))
		for _, x := range paramFormats {
			b.int16(x)
		}
	}

	b.int16(len(args))
	for _, x := range args {
		if x == nil {
			b.int32(-1)
		} else {
			datum := binaryEncode(&cn.parameterStatus, x)
			b.int32(len(datum))
			b.bytes(datum)
		}
	}
}

func (cn *conn) sendBinaryModeQuery(query string, args []driver.Value) {
	if len(args) >= 65536 {
		errorf("got %d parameters but PostgreSQL only supports 65535 parameters", len(args))
	}

	b := cn.writeBuf('P')
	b.byte(0) // unnamed statement
	b.string(query)
	b.int16(0)

	b.next('B')
	b.int16(0) // unnamed portal and statement
	cn.sendBinaryParameters(b, args)
	b.bytes(colFmtDataAllText)

	b.next('D')
	b.byte('P')
	b.byte(0) // unnamed portal

	b.next('E')
	b.byte(0)
	b.int32(0)

	b.next('S')
	cn.send(b)
}

func (cn *conn) processParameterStatus(r *readBuf) {
	var err error

	param := r.string()
	switch param {
	case "server_version":
		var major1 int
		var major2 int
		_, err = fmt.Sscanf(r.string(), "%d.%d", &major1, &major2)
		if err == nil {
			cn.parameterStatus.serverVersion = major1*10000 + major2*100
		}

	case "TimeZone":
		cn.parameterStatus.currentLocation, err = time.LoadLocation(r.string())
		if err != nil {
			cn.parameterStatus.currentLocation = nil
		}

	default:
		// ignore
	}
}

func (cn *conn) processReadyForQuery(r *readBuf) {
	cn.txnStatus = transactionStatus(r.byte())
}

func (cn *conn) readReadyForQuery() {
	t, r := cn.recv1()
	switch t {
	case 'Z':
		cn.processReadyForQuery(r)
		return
	default:
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected message %q; expected ReadyForQuery", t)
	}
}

func (cn *conn) processBackendKeyData(r *readBuf) {
	cn.processID = r.int32()
	cn.secretKey = r.int32()
}

func (cn *conn) readParseResponse() {
	t, r := cn.recv1()
	switch t {
	case '1':
		return
	case 'E':
		err := parseError(r)
		cn.readReadyForQuery()
		panic(err)
	default:
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected Parse response %q", t)
	}
}

func (cn *conn) readStatementDescribeResponse() (paramTyps []oid.Oid, colNames []string, colTyps []fieldDesc) {
	for {
		t, r := cn.recv1()
		switch t {
		case 't':
			nparams := r.int16()
			paramTyps = make([]oid.Oid, nparams)
			for i := range paramTyps {
				paramTyps[i] = r.oid()
			}
		case 'n':
			return paramTyps, nil, nil
		case 'T':
			colNames, colTyps = parseStatementRowDescribe(r)
			return paramTyps, colNames, colTyps
		case 'E':
			err := parseError(r)
			cn.readReadyForQuery()
			panic(err)
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unexpected Describe statement response %q", t)
		}
	}
}

func (cn *conn) readPortalDescribeResponse() rowsHeader {
	t, r := cn.recv1()
	switch t {
	case 'T':
		return parsePortalRowDescribe(r)
	case 'n':
		return rowsHeader{}
	case 'E':
		err := parseError(r)
		cn.readReadyForQuery()
		panic(err)
	default:
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected Describe response %q", t)
	}
	panic("not reached")
}

func (cn *conn) readBindResponse() {
	t, r := cn.recv1()
	switch t {
	case '2':
		return
	case 'E':
		err := parseError(r)
		cn.readReadyForQuery()
		panic(err)
	default:
		cn.err.set(driver.ErrBadConn)
		errorf("unexpected Bind response %q", t)
	}
}

func (cn *conn) postExecuteWorkaround() {
	// Work around a bug in sql.DB.QueryRow: in Go 1.2 and earlier it ignores
	// any errors from rows.Next, which masks errors that happened during the
	// execution of the query.  To avoid the problem in common cases, we wait
	// here for one more message from the database.  If it's not an error the
	// query will likely succeed (or perhaps has already, if it's a
	// CommandComplete), so we push the message into the conn struct; recv1
	// will return it as the next message for rows.Next or rows.Close.
	// However, if it's an error, we wait until ReadyForQuery and then return
	// the error to our caller.
	for {
		t, r := cn.recv1()
		switch t {
		case 'E':
			err := parseError(r)
			cn.readReadyForQuery()
			panic(err)
		case 'C', 'D', 'I':
			// the query didn't fail, but we can't process this message
			cn.saveMessage(t, r)
			return
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unexpected message during extended query execution: %q", t)
		}
	}
}

// Only for Exec(), since we ignore the returned data
func (cn *conn) readExecuteResponse(protocolState string) (res driver.Result, commandTag string, err error) {
	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			if err != nil {
				cn.err.set(driver.ErrBadConn)
				errorf("unexpected CommandComplete after error %s", err)
			}
			res, commandTag = cn.parseComplete(r.string())
		case 'Z':
			cn.processReadyForQuery(r)
			if res == nil && err == nil {
				err = errUnexpectedReady
			}
			return res, commandTag, err
		case 'E':
			err = parseError(r)
		case 'T', 'D', 'I':
			if err != nil {
				cn.err.set(driver.ErrBadConn)
				errorf("unexpected %q after error %s", t, err)
			}
			if t == 'I' {
				res = emptyRows
			}
			// ignore any results
		default:
			cn.err.set(driver.ErrBadConn)
			errorf("unknown %s response: %q", protocolState, t)
		}
	}
}

func parseStatementRowDescribe(r *readBuf) (colNames []string, colTyps []fieldDesc) {
	n := r.int16()
	colNames = make([]string, n)
	colTyps = make([]fieldDesc, n)
	for i := range colNames {
		colNames[i] = r.string()
		r.next(6)
		colTyps[i].OID = r.oid()
		colTyps[i].Len = r.int16()
		colTyps[i].Mod = r.int32()
		// format code not known when describing a statement; always 0
		r.next(2)
	}
	return
}

func parsePortalRowDescribe(r *readBuf) rowsHeader {
	n := r.int16()
	colNames := make([]string, n)
	colFmts := make([]format, n)
	colTyps := make([]fieldDesc, n)
	for i := range colNames {
		colNames[i] = r.string()
		r.next(6)
		colTyps[i].OID = r.oid()
		colTyps[i].Len = r.int16()
		colTyps[i].Mod = r.int32()
		colFmts[i] = format(r.int16())
	}
	return rowsHeader{
		colNames: colNames,
		colFmts:  colFmts,
		colTyps:  colTyps,
	}
}

// parseEnviron tries to mimic some of libpq's environment handling
//
// To ease testing, it does not directly reference os.Environ, but is
// designed to accept its output.
//
// Environment-set connection information is intended to have a higher
// precedence than a library default but lower than any explicitly
// passed information (such as in the URL or connection string).
func parseEnviron(env []string) (out map[string]string) {
	out = make(map[string]string)

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		accrue := func(keyname string) {
			out[keyname] = parts[1]
		}
		unsupported := func() {
			panic(fmt.Sprintf("setting %v not supported", parts[0]))
		}

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual. Unsupported but well-defined
		// keys cause a panic; these should be unset prior to
		// execution. Options which pq expects to be set to a
		// certain value are allowed, but must be set to that
		// value if present (they can, of course, be absent).
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			unsupported()
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
		case "PGSERVICE", "PGSERVICEFILE", "PGREALM":
			unsupported()
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGSSLCERT":
			accrue("sslcert")
		case "PGSSLKEY":
			accrue("sslkey")
		case "PGSSLROOTCERT":
			accrue("sslrootcert")
		case "PGSSLSNI":
			accrue("sslsni")
		case "PGREQUIRESSL", "PGSSLCRL":
			unsupported()
		case "PGREQUIREPEER":
			unsupported()
		case "PGKRBSRVNAME", "PGGSSLIB":
			unsupported()
		case "PGCONNECT_TIMEOUT":
			accrue("connect_timeout")
		case "PGCLIENTENCODING":
			accrue("client_encoding")
		case "PGDATESTYLE":
			accrue("datestyle")
		case "PGTZ":
			accrue("timezone")
		case "PGGEQO":
			accrue("geqo")
		case "PGSYSCONFDIR", "PGLOCALEDIR":
			unsupported()
		}
	}

	return out
}

// isUTF8 returns whether name is a fuzzy variation of the string "UTF-8".
func isUTF8(name string) bool {
	// Recognize all sorts of silly things as "UTF-8", like Postgres does
	s := strings.Map(alnumLowerASCII, name)
	return s == "utf8" || s == "unicode"
}

func alnumLowerASCII(ch rune) rune {
	if 'A' <= ch && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	if 'a' <= ch && ch <= 'z' || '0' <= ch && ch <= '9' {
		return ch
	}
	return -1 // discard
}
