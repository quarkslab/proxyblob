package socks5

import (
	"proxyblob/internal/mux"
	"testing"
)

func TestUDPHeaderRejectsMalformedAndFragmentedDatagrams(t *testing.T) {
	for _, packet := range [][]byte{nil, {0}, {0, 0, 0}, {1, 0, 0, 1, 127, 0, 0, 1, 0, 9}, {0, 1, 0, 1, 127, 0, 0, 1, 0, 9}, {0, 0, 1, 1, 127, 0, 0, 1, 0, 9}, {0, 0, 128, 1, 127, 0, 0, 1, 0, 9}, {0, 0, 0, 1, 127}, {0, 0, 0, 4, 0, 0}, {0, 0, 0, 3, 0, 0, 9}, {0, 0, 0, 3, 4, 'a', 0, 9}, {0, 0, 0, 99}} {
		if _, _, code := ExtractUDPHeader(packet); code == mux.ErrNone {
			t.Fatalf("accepted malformed packet %v", packet)
		}
	}
}
