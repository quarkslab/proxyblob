//go:build !windows && !js

package bootstrap

import "net/url"

// systemProxy: outside Windows the environment variables are the whole
// configuration, so there is nothing further to consult.
func systemProxy(*url.URL) (*url.URL, error) { return nil, nil }
