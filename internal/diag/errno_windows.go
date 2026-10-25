//go:build windows

package diag

import (
	"errors"
	"syscall"
)

// Winsock reports its own error numbers, which never equal the portable
// syscall.ECONNRESET family that Go defines on Windows. Classify them here so a
// peer reset on a Windows agent is not reported as a generic transport error.
func platformStreamCode(err error) byte {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return ErrNone
	}
	switch errno {
	case syscall.WSAECONNRESET, syscall.WSAECONNABORTED:
		return ErrStreamReset
	case 10057: // WSAENOTCONN
		return ErrStreamNotConnected
	case 10058: // WSAESHUTDOWN
		return ErrStreamBrokenPipe
	case 10060: // WSAETIMEDOUT
		return ErrTransportTimeout
	}
	return ErrNone
}

// platformDialCode classifies the Winsock errors a failed TCP dial returns.
func platformDialCode(err error) byte {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return ErrNone
	}
	switch errno {
	case 10061: // WSAECONNREFUSED
		return ErrConnectionRefused
	case 10065: // WSAEHOSTUNREACH
		return ErrHostUnreachable
	case 10051: // WSAENETUNREACH
		return ErrNetworkUnreachable
	case 10060: // WSAETIMEDOUT
		return ErrTTLExpired
	}
	return ErrNone
}
