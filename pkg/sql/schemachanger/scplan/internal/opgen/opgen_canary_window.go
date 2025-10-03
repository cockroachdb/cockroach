package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.CanaryWindow)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.CanaryWindow) *scop.SetCanaryWindow {
					return &scop.SetCanaryWindow{TableID: this.TableID}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.CanaryWindow) *scop.SetCanaryWindow {
					return &scop.SetCanaryWindow{TableID: this.TableID}
				}),
			),
		),
	)
}
