//go:build js

package bootstrap

import "github.com/atsika/aznet"

// ProxyOptions is empty under WASM: the host runtime owns networking and its
// own proxy settings.
func ProxyOptions() []aznet.Option { return nil }
