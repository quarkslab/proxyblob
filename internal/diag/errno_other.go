//go:build !windows

package diag

func platformStreamCode(error) byte { return ErrNone }

func platformDialCode(error) byte { return ErrNone }
