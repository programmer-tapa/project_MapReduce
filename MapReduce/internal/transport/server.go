package transport

import (
	"context"
	"net"

	"google.golang.org/grpc"

	"mapreduce/internal/core"
	"mapreduce/internal/transport/pb"
)

// GRPCServer wraps a core.Scheduler and exposes it as a gRPC service.
// Runs inside the coordinator process.
type GRPCServer struct {
	pb.UnimplementedMapReduceServiceServer
	scheduler  core.Scheduler
	listenAddr string
	server     *grpc.Server
}

// NewGRPCServer creates a gRPC server bound to the given scheduler.
func NewGRPCServer(scheduler core.Scheduler, listenAddr string) *GRPCServer {
	return &GRPCServer{
		scheduler:  scheduler,
		listenAddr: listenAddr,
	}
}

// Serve starts listening for incoming gRPC connections.
// Blocks until the server is stopped.
func (s *GRPCServer) Serve() error {
	lis, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	pb.RegisterMapReduceServiceServer(s.server, s)

	return s.server.Serve(lis)
}

// Stop gracefully shuts down the gRPC server.
func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// GetTask is called by a worker to request the next available task.
func (s *GRPCServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.GetTaskResponse, error) {
	assignment := s.scheduler.AssignTask()

	var taskType pb.TaskType
	switch assignment.Type {
	case core.MapTask:
		taskType = pb.TaskType_MAP_TASK
	case core.ReduceTask:
		taskType = pb.TaskType_REDUCE_TASK
	case core.WaitTask:
		taskType = pb.TaskType_WAIT_TASK
	case core.ExitTask:
		taskType = pb.TaskType_EXIT_TASK
	}

	return &pb.GetTaskResponse{
		TaskType: taskType,
		TaskId:   int32(assignment.TaskID),
		Filename: assignment.Filename,
		NReduce:  int32(assignment.NReduce),
		NMaps:    int32(assignment.NMaps),
	}, nil
}

// ReportTask is called by a worker to report task completion or failure.
func (s *GRPCServer) ReportTask(ctx context.Context, req *pb.ReportTaskRequest) (*pb.ReportTaskResponse, error) {
	var coreTaskType core.TaskType
	switch req.TaskType {
	case pb.TaskType_MAP_TASK:
		coreTaskType = core.MapTask
	case pb.TaskType_REDUCE_TASK:
		coreTaskType = core.ReduceTask
	}

	report := core.TaskReport{
		Type:   coreTaskType,
		TaskID: int(req.TaskId),
	}

	s.scheduler.CompleteTask(report)
	return &pb.ReportTaskResponse{}, nil
}

// Heartbeat is an optional keep-alive signal from workers.
func (s *GRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}
