package transport

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"mapreduce/internal/core"
	"mapreduce/internal/transport/pb"
)

// GRPCClient wraps a gRPC connection to the coordinator.
// Used by the worker to request tasks and report completions.
type GRPCClient struct {
	conn   *grpc.ClientConn
	client pb.MapReduceServiceClient
}

// NewGRPCClient creates a client connected to the coordinator at the given address.
func NewGRPCClient(coordinatorAddr string) (*GRPCClient, error) {
	conn, err := grpc.Dial(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := pb.NewMapReduceServiceClient(conn)
	return &GRPCClient{
		conn:   conn,
		client: client,
	}, nil
}

// GetTask requests a task assignment from the coordinator.
func (c *GRPCClient) GetTask() (core.TaskAssignment, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.GetTask(ctx, &pb.GetTaskRequest{WorkerId: "worker-client"})
	if err != nil {
		return core.TaskAssignment{}, err
	}

	var coreTaskType core.TaskType
	switch resp.TaskType {
	case pb.TaskType_MAP_TASK:
		coreTaskType = core.MapTask
	case pb.TaskType_REDUCE_TASK:
		coreTaskType = core.ReduceTask
	case pb.TaskType_WAIT_TASK:
		coreTaskType = core.WaitTask
	case pb.TaskType_EXIT_TASK:
		coreTaskType = core.ExitTask
	}

	return core.TaskAssignment{
		Type:     coreTaskType,
		TaskID:   int(resp.TaskId),
		Filename: resp.Filename,
		NReduce:  int(resp.NReduce),
		NMaps:    int(resp.NMaps),
	}, nil
}

// ReportTask notifies the coordinator that a task has been completed.
func (c *GRPCClient) ReportTask(report core.TaskReport) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var taskType pb.TaskType
	switch report.Type {
	case core.MapTask:
		taskType = pb.TaskType_MAP_TASK
	case core.ReduceTask:
		taskType = pb.TaskType_REDUCE_TASK
	}

	req := &pb.ReportTaskRequest{
		WorkerId: "worker-client",
		TaskType: taskType,
		TaskId:   int32(report.TaskID),
		Success:  true,
	}

	_, err := c.client.ReportTask(ctx, req)
	return err
}

// Close closes the gRPC connection.
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
