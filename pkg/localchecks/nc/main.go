package nc

import (
	"bytes"
	"context"
	"fmt"
	"net"

	"github.com/scorify/schema"
)

type Schema struct {
	Target         string `key:"target"`
	Port           int    `key:"port" default:"23"`
	Command        string `key:"command"`
	ExpectedOutput string `key:"expected_output"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.Target == "" {
		return fmt.Errorf("target is required")
	}

	if conf.Port < 1 || conf.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	if conf.Command == "" {
		return fmt.Errorf("command is required")
	}

	if conf.ExpectedOutput == "" {
		return fmt.Errorf("expected_output is required")
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	connStr := fmt.Sprintf("%s:%d", conf.Target, conf.Port)
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to %q: %w", connStr, err)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("failed to get deadline")
	}

	err = conn.SetDeadline(deadline)
	if err != nil {
		return fmt.Errorf("failed to set deadline: %w", err)
	}

	buffer := make([]byte, 2048)

	// read until the buffer is not 2048 bytes
	for ctx.Err() == nil {
		i, err := conn.Read(buffer)
		if err != nil {
			return fmt.Errorf("failed to read from %q: %w", connStr, err)
		}

		if i < 2048 {
			break
		}
	}

	_, err = conn.Write([]byte(conf.Command + "\n"))
	if err != nil {
		return fmt.Errorf("failed to write to %q: %w", connStr, err)
	}

	output := bytes.NewBuffer(nil)
	buffer = make([]byte, 2048)

	// read until the buffer is not 2048 bytes
	for ctx.Err() == nil {
		i, err := conn.Read(buffer)
		if err != nil {
			return fmt.Errorf("failed to read from %q: %w", connStr, err)
		}
		output.Write(buffer[:i])
		if output.Len() > 1<<20 {
			return fmt.Errorf("TCP response exceeds 1 MiB check limit")
		}

		if i < 2048 {
			break
		}
	}

	if !bytes.Contains(output.Bytes(), []byte(conf.ExpectedOutput)) {
		return fmt.Errorf("expected output %q not found in %q", conf.ExpectedOutput, output.String())
	}

	return nil
}
