package tcp

import (
	"context"
	"fmt"
	"net"

	"github.com/scorify/schema"
)

type Schema struct {
	Target string `key:"target"`
	Port   int    `key:"port"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.Target == "" {
		return fmt.Errorf("target is required; got %q", conf.Target)
	}

	if conf.Port <= 0 || conf.Port > 65535 {
		return fmt.Errorf("provided invalid port: %d", conf.Port)
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

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to %q; %w", connStr, err)
	}
	defer conn.Close()

	return nil
}
