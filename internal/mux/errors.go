package mux

import (
	"fmt"
	"os"

	"proxyblob/internal/diag"

	"github.com/google/uuid"
)

// ReportError does not alter delivery, close, or the wire protocol. Descriptions
// belong to the proxy callback; the default agent output contains only a code.
func (h *BaseHandler) ReportError(id uuid.UUID, code byte) {
	if code == diag.ErrNone {
		return
	}
	if h.OnError != nil {
		h.OnError(id, code)
		return
	}
	fmt.Fprintln(os.Stderr, code)
}
