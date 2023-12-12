

package statuspb

import (
	"github.com/cockroachdb/redact"
)

func (h HealthAlert) String() string {
	return redact.StringWithoutMarkers(h)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (h HealthAlert) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("{StoreID:%d Category:%d Description:%s Value:%f}",
		h.StoreID, redact.Safe(h.Category), redact.Safe(h.Description), h.Value)
}

func (h HealthCheckResult) String() string {
	return redact.StringWithoutMarkers(h)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (h HealthCheckResult) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("{Alerts:[")
	for _, alert := range h.Alerts {
		w.Printf("%s,", alert)
	}
	w.Printf("]}")
}