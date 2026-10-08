package bootstrap

import (
	"bytes"
	"strings"
	"testing"
)

func TestIdentityRoundTripAndTruncation(t *testing.T) {
	for _, identity := range []string{"user@host", strings.Repeat("x", MaxIdentityLen+40)} {
		var b bytes.Buffer
		if err := WriteIdentity(&b, identity); err != nil {
			t.Fatal(err)
		}
		got, err := ReadIdentity(&b)
		want := identity[:min(len(identity), MaxIdentityLen)]
		if err != nil || got != want || b.Len() != 0 {
			t.Fatalf("identity %d bytes: got %d bytes, %v, %d left", len(identity), len(got), err, b.Len())
		}
	}
}

func TestReadIdentityRejectsBadFrames(t *testing.T) {
	for name, frame := range map[string][]byte{
		"empty":     {0, 0},
		"oversized": {0x02, 0x01},
		"short":     {0, 5, 'a', 'b'},
		"no length": {0},
	} {
		if _, err := ReadIdentity(bytes.NewReader(frame)); err == nil {
			t.Errorf("%s frame accepted", name)
		}
	}
}

func TestConnectionStringRoundTrip(t *testing.T) {
	h, tok := Endpoints("listener")
	encoded, err := Encode("azblob", "https://account.invalid/?sv=1&"+h+"=a&"+tok+"=b", h, tok)
	if err != nil {
		t.Fatal(err)
	}
	driver, address, err := Decode(encoded)
	if err != nil || driver != "azblob" {
		t.Fatalf("decode: %q %v", driver, err)
	}
	if opts, err := DialOptions(address); err != nil || len(opts) != 1 {
		t.Fatalf("namespace not carried: %v", err)
	}
	if _, _, err := Decode(""); err != ErrEmpty {
		t.Fatalf("empty: %v", err)
	}
	if _, _, err := Decode("not base64!"); err != ErrInvalid {
		t.Fatalf("invalid: %v", err)
	}
}
