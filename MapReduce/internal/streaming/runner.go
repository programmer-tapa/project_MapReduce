package streaming

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"time"
)

// Runner spawns and manages an external subprocess for Map or Reduce operations.
type Runner struct {
	command string
	args    []string
}

// NewRunner creates a subprocess runner for the given command.
func NewRunner(command string, args ...string) *Runner {
	return &Runner{
		command: command,
		args:    args,
	}
}

// Run starts the subprocess, writes input to its stdin, and returns a reader
// for its stdout. The caller is responsible for closing the returned reader.
func (r *Runner) Run(input io.Reader) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	cmd := exec.CommandContext(ctx, r.command, r.args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}

	go func() {
		defer stdin.Close()
		if input != nil {
			_, _ = io.Copy(stdin, input)
		}
	}()

	return &subprocessReader{
		stdout: stdout,
		cmd:    cmd,
		cancel: cancel,
	}, nil
}

type subprocessReader struct {
	stdout io.ReadCloser
	cmd    *exec.Cmd
	cancel context.CancelFunc
}

func (s *subprocessReader) Read(p []byte) (n int, err error) {
	return s.stdout.Read(p)
}

func (s *subprocessReader) Close() error {
	defer s.cancel()
	err := s.stdout.Close()
	_ = s.cmd.Wait()
	return err
}
