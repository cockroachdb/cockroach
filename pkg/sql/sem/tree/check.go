// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

type CheckExternalConnection struct {
	URI     Expr
	Options CheckExternalConnectionOptions
}

func (c *CheckExternalConnection) Format(ctx *FmtCtx) {
	ctx.WriteString("CHECK EXTERNAL CONNECTION ")
	ctx.FormatURI(c.URI)
	if !c.Options.IsDefault() {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&c.Options)
		ctx.WriteString(")")
	}
}

var _ Statement = &CheckExternalConnection{}

type CheckExternalConnectionOptions struct {
	TransferSize Expr
	Duration     Expr
	Concurrency  Expr
}

var _ NodeFormatter = &CheckExternalConnectionOptions{}

func (c *CheckExternalConnectionOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}
	if c.Concurrency != nil {
		maybeAddSep()
		ctx.WriteString("CONCURRENTLY = ")
		ctx.FormatNode(c.Concurrency)
	}
	if c.TransferSize != nil {
		maybeAddSep()
		ctx.WriteString("TRANSFER = ")
		ctx.FormatNode(c.TransferSize)
	}
	if c.Duration != nil {
		maybeAddSep()
		ctx.WriteString("TIME = ")
		ctx.FormatNode(c.Duration)
	}
}

func (c *CheckExternalConnectionOptions) CombineWith(other *CheckExternalConnectionOptions) error {
	var err error
	c.TransferSize, err = combineExpr(c.TransferSize, other.TransferSize, "transfer")
	if err != nil {
		return err
	}
	c.Concurrency, err = combineExpr(c.Concurrency, other.Concurrency, "concurrently")
	if err != nil {
		return err
	}
	c.Duration, err = combineExpr(c.Duration, other.Duration, "time")
	if err != nil {
		return err
	}
	return nil
}

// IsDefault returns true if this options object is empty.
func (c *CheckExternalConnectionOptions) IsDefault() bool {
	options := CheckExternalConnectionOptions{}
	return c.Duration == options.Duration &&
		c.TransferSize == options.TransferSize &&
		c.Concurrency == options.Concurrency
}
