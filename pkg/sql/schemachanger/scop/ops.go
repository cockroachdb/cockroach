package scop

type Type int

const (
	_ Type = iota
	DescriptorMutationType
	BackfillType
	ValidationType
)

type MutationOps []MutationOp

func (m MutationOps) Len() int      { return len(m) }
func (m MutationOps) At(idx int) Op { return m[idx] }

type BackfillOps []BackfillOp

func (b BackfillOps) Len() int      { return len(b) }
func (b BackfillOps) At(idx int) Op { return b[idx] }

type ValidationOps []ValidationOp

func (v ValidationOps) Len() int      { return len(v) }
func (v ValidationOps) At(idx int) Op { return v[idx] }

var _ Ops = (MutationOps)(nil)
var _ Ops = (BackfillOps)(nil)
var _ Ops = (ValidationOps)(nil)

// Op represents an action to be taken on a single descriptor.
type Op interface {
	op()
	Type() Type
}

type Ops interface {
	Len() int
	At(idx int) Op
}

type baseOp struct{}

func (baseOp) op() {}
