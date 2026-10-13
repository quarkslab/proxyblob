//go:build windows

package bootstrap

import (
	"net/http"
	"net/url"

	"github.com/mattn/go-ieproxy"
)

// systemProxy reads the settings Windows applications share: the registry's
// Internet Options, an explicit PAC file, and WPAD auto-detection (evaluated
// by WinHTTP for each lookup). The registry is read once, when first needed.
// A PAC answer that lists several proxies yields only the first.
func systemProxy(target *url.URL) (*url.URL, error) {
	return ieproxy.GetProxyFunc()(&http.Request{URL: pacURL(target)})
}
