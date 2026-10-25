//go:build windows

package bootstrap

import (
	"net/url"
	"strings"
	"sync"
	"unsafe"

	"github.com/mattn/go-ieproxy"
	"golang.org/x/sys/windows"
)

// systemRoute reads the settings Windows applications share: the registry's
// Internet Options, an explicit PAC file, and WPAD auto-detection. The
// registry is read once, when first needed; a PAC file is evaluated by
// WinHTTP for each lookup. A PAC file that cannot be fetched or run falls
// back to the static proxy, as browsers do.
func systemRoute(target *url.URL) (route, error) {
	conf := ieproxy.GetConf()
	if conf.Automatic.Active {
		if proxies, ok := pacProxies(conf.Automatic.PreConfiguredURL, pacURL(target)); ok {
			byDefault := len(proxies) > 0 && sameProxies(proxies, pacDefault(conf.Automatic.PreConfiguredURL))
			return route{proxies: proxies, byDefault: byDefault}, nil
		}
	}
	if !conf.Static.Active {
		return route{}, nil
	}
	server, ok := conf.Static.Protocols[target.Scheme]
	if !ok {
		server = conf.Static.Protocols[""]
	}
	proxy := parseProxy(server)
	if proxy == nil || bypassed(target, strings.Split(conf.Static.NoProxy, ",")) {
		return route{}, nil
	}
	return route{proxies: []*url.URL{proxy}, byDefault: true}, nil
}

// pacProbe matches no rule a PAC file could sensibly have, so its answer is
// the file's catch-all.
var pacProbe = &url.URL{Scheme: "https", Host: "pac-default.invalid", Path: "/"}

var (
	pacDefaultOnce sync.Once
	pacDefaultList []*url.URL
)

// pacDefault is the PAC file's answer for destinations it does not single
// out, asked once. Nil when it cannot be told, so every proxy it names then
// counts as specific, as before.
func pacDefault(pacFile string) []*url.URL {
	pacDefaultOnce.Do(func() {
		pacDefaultList, _ = pacProxies(pacFile, pacProbe)
	})
	return pacDefaultList
}

func sameProxies(a, b []*url.URL) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].String() != b[i].String() {
			return false
		}
	}
	return true
}

var (
	winhttp                   = windows.NewLazySystemDLL("winhttp.dll")
	procWinHttpOpen           = winhttp.NewProc("WinHttpOpen")
	procWinHttpCloseHandle    = winhttp.NewProc("WinHttpCloseHandle")
	procWinHttpGetProxyForURL = winhttp.NewProc("WinHttpGetProxyForUrl")
	procGlobalFree            = windows.NewLazySystemDLL("kernel32.dll").NewProc("GlobalFree")
)

// WINHTTP_AUTOPROXY_OPTIONS
type autoProxyOptions struct {
	flags                 uint32
	autoDetectFlags       uint32
	autoConfigURL         *uint16
	reserved              uintptr
	reserved2             uint32
	autoLogonIfChallenged int32
}

// WINHTTP_PROXY_INFO
type proxyInfo struct {
	accessType uint32
	proxy      *uint16
	bypass     *uint16
}

const (
	autoProxyAutoDetect = 0x1
	autoProxyConfigURL  = 0x2
	autoDetectDHCP      = 0x1
	autoDetectDNSA      = 0x2
	accessNamedProxy    = 3
)

// pacProxies asks WinHTTP which proxies the PAC file (or WPAD, when pacFile
// is empty) names for target, in failover order. ok is false when the answer
// could not be had; an empty list with ok means DIRECT.
func pacProxies(pacFile string, target *url.URL) (proxies []*url.URL, ok bool) {
	targetPtr, err := windows.UTF16PtrFromString(target.String())
	if err != nil {
		return nil, false
	}
	opts := autoProxyOptions{flags: autoProxyAutoDetect, autoDetectFlags: autoDetectDHCP | autoDetectDNSA, autoLogonIfChallenged: 1}
	if pacFile != "" {
		if opts.autoConfigURL, err = windows.UTF16PtrFromString(pacFile); err != nil {
			return nil, false
		}
		opts.flags, opts.autoDetectFlags = autoProxyConfigURL, 0
	}
	session, _, _ := procWinHttpOpen.Call(0, 0, 0, 0, 0)
	if session == 0 {
		return nil, false
	}
	defer procWinHttpCloseHandle.Call(session)
	var info proxyInfo
	ret, _, _ := procWinHttpGetProxyForURL.Call(session, uintptr(unsafe.Pointer(targetPtr)), uintptr(unsafe.Pointer(&opts)), uintptr(unsafe.Pointer(&info)))
	if info.proxy != nil {
		defer procGlobalFree.Call(uintptr(unsafe.Pointer(info.proxy)))
	}
	if info.bypass != nil {
		defer procGlobalFree.Call(uintptr(unsafe.Pointer(info.bypass)))
	}
	if ret == 0 {
		return nil, false
	}
	if info.accessType != accessNamedProxy || info.proxy == nil {
		return nil, true
	}
	return parseProxyList(windows.UTF16PtrToString(info.proxy)), true
}
