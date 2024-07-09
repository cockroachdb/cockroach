package tree

type CreateTrigger struct {
	Replace     bool
	Name        Name
	ActionTime  TriggerActionTime
	Events      []*TriggerEvent
	TableName   *UnresolvedObjectName
	Transitions []*TriggerTransition
	ForEach     TriggerForEach
	When        Expr
	FuncName    ResolvableFunctionReference
	FuncArgs    []string
}

var _ Statement = &CreateTrigger{}

// Format implements the NodeFormatter interface.
func (node *CreateTrigger) Format(ctx *FmtCtx) {
	if node.Replace {
		ctx.WriteString("CREATE OR REPLACE TRIGGER ")
	} else {
		ctx.WriteString("CREATE TRIGGER ")
	}
	node.Name.Format(ctx)
	ctx.WriteString(" ")
	node.ActionTime.Format(ctx)
	ctx.WriteString(" ")
	for i := range node.Events {
		if i > 0 {
			ctx.WriteString(" OR ")
		}
		node.Events[i].Format(ctx)
	}
	ctx.WriteString(" ON ")
	ctx.FormatNode(node.TableName)
	if len(node.Transitions) > 0 {
		ctx.WriteString(" REFERENCING ")
		for i := range node.Transitions {
			if i > 0 {
				ctx.WriteString(" ")
			}
			node.Transitions[i].Format(ctx)
		}
	}
	ctx.WriteString(" ")
	node.ForEach.Format(ctx)
	if node.When != nil {
		ctx.WriteString(" WHEN ")
		ctx.FormatNode(node.When)
	}
	ctx.WriteString(" EXECUTE FUNCTION ")
	ctx.FormatNode(&node.FuncName)
	ctx.WriteString("(")
	for i, arg := range node.FuncArgs {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatStringConstant(arg)
	}
	ctx.WriteString(")")
}

type TriggerActionTime uint8

const (
	TriggerActionTimeBefore TriggerActionTime = iota
	TriggerActionTimeAfter
	TriggerActionTimeInsteadOf
)

// Format implements the NodeFormatter interface.
func (node *TriggerActionTime) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerActionTimeBefore:
		ctx.WriteString("BEFORE")
	case TriggerActionTimeAfter:
		ctx.WriteString("AFTER")
	case TriggerActionTimeInsteadOf:
		ctx.WriteString("INSTEAD OF")
	}
}

type TriggerEventType uint8

const (
	TriggerEventInsert TriggerEventType = 1 << iota
	TriggerEventUpdate
	TriggerEventDelete
	TriggerEventTruncate
	TriggerEventUpsert
)

// Format implements the NodeFormatter interface.
func (node *TriggerEventType) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerEventInsert:
		ctx.WriteString("INSERT")
	case TriggerEventUpdate:
		ctx.WriteString("UPDATE")
	case TriggerEventDelete:
		ctx.WriteString("DELETE")
	case TriggerEventTruncate:
		ctx.WriteString("TRUNCATE")
	case TriggerEventUpsert:
		ctx.WriteString("UPSERT")
	}
}

type TriggerEvent struct {
	EventType TriggerEventType
	Columns   NameList
}

// Format implements the NodeFormatter interface.
func (node *TriggerEvent) Format(ctx *FmtCtx) {
	node.EventType.Format(ctx)
	if len(node.Columns) > 0 {
		ctx.WriteString(" OF ")
		ctx.FormatNode(&node.Columns)
	}
}

type TriggerTransition struct {
	Name  Name
	IsNew bool
	IsRow bool
}

// Format implements the NodeFormatter interface.
func (node *TriggerTransition) Format(ctx *FmtCtx) {
	if node.IsNew {
		ctx.WriteString("NEW")
	} else {
		ctx.WriteString("OLD")
	}
	if node.IsRow {
		ctx.WriteString(" ROW")
	} else {
		ctx.WriteString(" TABLE")
	}
	ctx.WriteString(" AS ")
	node.Name.Format(ctx)
}

type TriggerForEach uint8

const (
	TriggerForEachStatement TriggerForEach = iota
	TriggerForEachRow
)

// Format implements the NodeFormatter interface.
func (node *TriggerForEach) Format(ctx *FmtCtx) {
	switch *node {
	case TriggerForEachStatement:
		ctx.WriteString("FOR EACH STATEMENT")
	case TriggerForEachRow:
		ctx.WriteString("FOR EACH ROW")
	}
}
