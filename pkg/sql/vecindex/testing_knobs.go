package vecindex

import "github.com/cockroachdb/cockroach/pkg/base"

type VecIndexTestingKnobs struct {
	DuringVecIndexPull func()
	BeforeVecIndexWait func()
}

var _ base.ModuleTestingKnobs = (*VecIndexTestingKnobs)(nil)

func (VecIndexTestingKnobs) ModuleTestingKnobs() {}
