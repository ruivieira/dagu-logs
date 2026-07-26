# dagu-logs

[![ci](https://github.com/ruivieira/dagu-logs/actions/workflows/ci.yml/badge.svg)](https://github.com/ruivieira/dagu-logs/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](https://go.dev/)

<p align="center">
  <img src="docs/image.jpg" alt="dagu-logs" width="720" />
</p>

Minimal CLI app to colour and live-tail Dagu DAG step logs. When you run `dagu-logs start …`, it starts Dagu as usual and streams each step’s `.out` / sub-DAG `.err` output with progress labels instead of waiting for the final tree.

## Requirements

- Go 1.26+
- `dagu` on `PATH` for real runs

## Install

From GitHub (installs to `$(go env GOPATH)/bin`):

```bash
go install github.com/ruivieira/dagu-logs@latest
```

From a local checkout:

```bash
make install
# → ~/.local/bin/dagu-logs
```

Or:

```bash
go install .
```

## Usage

```bash
dagu-logs start path/to/dag.yaml -- key=value other=./relative
```

- Relative parameter values after `--` (`.`, `..`, `./…`, `../…`) are resolved to absolute paths before Dagu starts.
- Any command other than `start` (or `start` without a `.yaml`/`.yml` file) is passed straight through to `dagu`.

## Development

```bash
make build
go test ./... -race -count=1
```

CI runs tests, `go vet`, golangci-lint, gosec, and govulncheck on every push and pull request.
