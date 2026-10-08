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
		if diag.IsStreamClosure(code) {
			event = logger.Debug()
		}
		event.Uint8("code", code).Str("conn_id", id.String()).Msg(diag.Description(code))
	}
}
