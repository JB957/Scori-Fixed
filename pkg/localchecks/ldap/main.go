package ldap

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/scorify/schema"
)

type Schema struct {
	Server   string `key:"server"`
	Port     int    `key:"port" default:"389"`
	Username string `key:"username"`
	Password string `key:"password"`
	LDAPS    bool   `key:"ldaps" default:"false"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if conf.Server == "" {
		return fmt.Errorf("server is required; got %q", conf.Server)
	}

	if conf.Port <= 0 || conf.Port > 65535 {
		return fmt.Errorf("port is invalid; got %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username is required; got %q", conf.Username)
	}

	if conf.Password == "" {
		return fmt.Errorf("password is required; got %q", conf.Password)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	var connStr string
	if conf.LDAPS {
		connStr = fmt.Sprintf("ldaps://%s:%d?tls=1&insecure=1", conf.Server, conf.Port)
	} else {
		connStr = fmt.Sprintf("ldap://%s:%d", conf.Server, conf.Port)
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("context deadline is not set")
	}

	dialer := &net.Dialer{
		Deadline: deadline,
	}

	conn, err := ldap.DialURL(connStr, ldap.DialWithDialer(dialer))
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	conn.SetTimeout(time.Until(deadline))
	err = conn.Bind(conf.Username, conf.Password)
	if err != nil {
		return fmt.Errorf("failed to bind: %w", err)
	}

	return nil
}
