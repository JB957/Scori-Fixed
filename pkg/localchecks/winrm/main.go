package winrm

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/masterzen/winrm"
	"github.com/scorify/schema"
)

type outputLimit struct {
	mu        sync.Mutex
	remaining int
	exceeded  bool
	cancel    context.CancelFunc
}

type limitedWriter struct {
	limit *outputLimit
	buf   bytes.Buffer
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	w.limit.mu.Lock()
	defer w.limit.mu.Unlock()
	if len(p) > w.limit.remaining {
		w.limit.exceeded = true
		w.limit.cancel()
		return 0, fmt.Errorf("WinRM output exceeds 1 MiB check limit")
	}
	w.limit.remaining -= len(p)
	return w.buf.Write(p)
}

type Schema struct {
	Server         string `key:"server"`
	Port           int    `key:"port" default:"5985"`
	Username       string `key:"username"`
	Password       string `key:"password"`
	Command        string `key:"command"`
	ExpectedOutput string `key:"expected_output"`
	HTTPS          bool   `key:"https"`
	Insecure       bool   `key:"insecure"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.Server == "" {
		return fmt.Errorf("server is required, got %q", conf.Server)
	}

	if conf.Port >= 65536 || conf.Port <= 0 {
		return fmt.Errorf("valid port is required, got %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username is required; got %q", conf.Username)
	}

	if conf.Command == "" {
		return fmt.Errorf("command is required; got %q", conf.Command)
	}

	if conf.ExpectedOutput == "" {
		return fmt.Errorf("expected_output is required; got %q", conf.ExpectedOutput)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("failed to get context deadline")
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return context.DeadlineExceeded
	}
	endpoint := winrm.NewEndpoint(conf.Server, conf.Port, conf.HTTPS, conf.Insecure, nil, nil, nil, timeout)
	client, err := winrm.NewClient(endpoint, conf.Username, conf.Password)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	commandCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	limit := &outputLimit{remaining: 1 << 20, cancel: cancel}
	stdout := &limitedWriter{limit: limit}
	stderr := &limitedWriter{limit: limit}
	_, err = client.RunWithContext(commandCtx, conf.Command, stdout, stderr)
	limit.mu.Lock()
	exceeded := limit.exceeded
	stdoutText := stdout.buf.String()
	stderrText := stderr.buf.String()
	limit.mu.Unlock()
	if exceeded {
		return fmt.Errorf("WinRM output exceeds 1 MiB check limit")
	}
	if err != nil {
		return fmt.Errorf("failed to run command: %w", err)
	}
	if stderrText != "" {
		return fmt.Errorf("command returned error: %s", stderrText)
	}
	if strings.TrimSpace(stdoutText) != strings.TrimSpace(conf.ExpectedOutput) {
		return fmt.Errorf("expected output does not match actual output")
	}
	return nil
}
