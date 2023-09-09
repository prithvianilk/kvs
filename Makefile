GO=go

shell: cmd/*.go pkg/**/**.go
	$(GO) build -o ./shell ./cmd/shell.go

test:
	$(GO) test -v ./...

clean:
	rm -rf ./shell