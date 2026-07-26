BINARY  := dagu-logs
INSTALL := $(HOME)/.local/bin/$(BINARY)

.PHONY: build install

build:
	go build -o $(BINARY) .

install: build
	install -m 755 $(BINARY) $(INSTALL)
