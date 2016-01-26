//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package cli

// Context holds parameters needed to setup a server.
import "github.com/cockroachdb/cockroach/server"

// Calling "cli".initFlags(ctx *Context) will initialize Context using
// command flags. Keep in sync with "cli/flags.go".
type Context struct {
	// Embed the server context.
	server.Context

	// OneShotSQL indicates the SQL client should run the command-line
	// statement(s) and terminate directly, without presenting a REPL to
	// the user.
	OneShotSQL bool
}

// NewContext returns a Context with default values.
func NewContext() *Context {
	ctx := &Context{}
	ctx.InitDefaults()
	return ctx
}

// InitDefaults sets up the default values for a Context.
func (ctx *Context) InitDefaults() {
	ctx.Context.InitDefaults()
	ctx.OneShotSQL = false
}
