.PHONY: all clean proxy agent wasm

ifndef TOKEN
CONN_STRING ?=
else
CONN_STRING = -X 'main.ConnString=$(TOKEN)'
endif

all: clean proxy agent wasm

proxy:
	go build -ldflags="-s -w" -trimpath -o proxy ./cmd/proxy

agent:
	go build -ldflags="-s -w $(CONN_STRING)" -trimpath -o agent ./cmd/agent

wasm:
	GOOS=js GOARCH=wasm go build -ldflags="-s -w $(CONN_STRING)" -trimpath -o agent.wasm ./cmd/agent

clean:
	rm -f proxy agent agent.wasm
