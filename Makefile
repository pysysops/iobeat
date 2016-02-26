build:
	go build

.PHONY: test
test:
	go test ./...

.PHONY: clean
clean:
	rm iobeat || true
