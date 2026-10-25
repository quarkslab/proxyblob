package proxy

import (
	"proxyblob/internal/diag"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

func protocolErrorReporter(logger zerolog.Logger) func(uuid.UUID, byte) {
	return func(id uuid.UUID, code byte) {
		if code == diag.ErrNone {
			return
		}
		event := logger.Warn()
		// A packet that crossed its stream's close in flight; older agents report it.
		if diag.IsStreamClosure(code) || code == diag.ErrConnectionNotFound {
			event = logger.Debug()
		}
		event.Uint8("code", code).Str("conn_id", id.String()).Msg(diag.Description(code))
	}
}
