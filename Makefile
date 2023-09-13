GO=go

kvs: cmd/*.go pkg/**/**.go
	$(GO) build -o ./kvs ./cmd/kvs.go

test:
	$(GO) test -v ./...

clean:
	rm -rf ./kvs
	rm -rf ./test.db/*