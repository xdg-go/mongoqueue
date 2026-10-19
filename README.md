# mongoqueue

[![Go Reference](https://pkg.go.dev/badge/github.com/xdg-go/mongoqueue.svg)](https://pkg.go.dev/github.com/xdg-go/mongoqueue)
[![test](https://github.com/xdg-go/mongoqueue/actions/workflows/test.yml/badge.svg)](https://github.com/xdg-go/mongoqueue/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

mongoqueue is a fair, multi-tenant job queue on MongoDB for Go. It is a
durable lease primitive: callers enqueue jobs and run their own workers, and
the queue dispatches jobs fairly across tenants using virtual time, leases
each claimed job, and records how each one ends. It does not run workers,
retry, dead-letter, or orchestrate workflows; those belong to the caller.

## Install

```
go get github.com/xdg-go/mongoqueue
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
