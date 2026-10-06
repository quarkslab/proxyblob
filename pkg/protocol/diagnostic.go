package protocol

import (
	"fmt"
	"github.com/google/uuid"
	"os"
)

// ReportError does not alter delivery, close, or the wire protocol. Descriptions
// belong to the proxy callback; the default agent output contains only a code.
func (h *BaseHandler) ReportError(id uuid.UUID, code byte) {
	if code == ErrNone {
		return
	}
	if h.OnError != nil {
		h.OnError(id, code)
		return
	}
	fmt.Fprintln(os.Stderr, code)
}
