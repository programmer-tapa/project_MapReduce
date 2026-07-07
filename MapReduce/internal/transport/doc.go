// Package transport provides the gRPC server and client implementations
// that bridge the core.Scheduler (coordinator) and core.Executor (worker)
// interfaces over the network.
//
// This is the infrastructure boundary — it translates between protobuf messages
// and core domain types. The coordinator and worker packages never import
// this package; instead, cmd/ entrypoints wire transport to core interfaces.
//
// Components:
//   - server.go: gRPC server wrapping core.Scheduler (runs in coordinator process)
//   - client.go: gRPC client wrapping coordinator RPCs (runs in worker process)
package transport
