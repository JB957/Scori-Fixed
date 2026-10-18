package ssh

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/scorify/schema"
	"golang.org/x/crypto/ssh"
)

type limitedOutput struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	conn     net.Conn
	exceeded bool
}

func (o *limitedOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(p) > 1<<20-o.buf.Len() {
		o.exceeded = true
		_ = o.conn.Close()
		return 0, fmt.Errorf("SSH output exceeds 1 MiB check limit")
	}
	return o.buf.Write(p)
}

func (o *limitedOutput) Result() (string, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String(), o.exceeded
}

type Schema struct {
	Server         string `key:"server"`
	Port           int    `key:"port" default:"22"`
	Username       string `key:"username"`
	Password       string `key:"password"`
	Command        string `key:"command"`
	ExpectedOutput string `key:"expected_output"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.Server == "" {
		return fmt.Errorf("server is required; got %q", conf.Server)
	}

	if conf.Port <= 0 || conf.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535; got %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username is required; got %q", conf.Username)
	}

	if conf.Password == "" {
		return fmt.Errorf("password is required; got %q", conf.Password)
	}

	if conf.Command == "" {
		return fmt.Errorf("command is required; got %q", conf.Command)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	ssh_config := &ssh.ClientConfig{
		User: conf.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(conf.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	target := net.JoinHostPort(conf.Server, fmt.Sprint(conf.Port))
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return err
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	sshConn, chans, reqs, err := ssh.NewClientConn(conn, target, ssh_config)
	if err != nil {
		return err
	}
	client := ssh.NewClient(sshConn, chans, reqs)
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	output := &limitedOutput{conn: conn}
	session.Stdout = output
	session.Stderr = output
	err = session.Run(conf.Command)
	result, exceeded := output.Result()
	if exceeded {
		return fmt.Errorf("SSH output exceeds 1 MiB check limit")
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(result) != strings.TrimSpace(conf.ExpectedOutput) {
		return fmt.Errorf("command output does not match expected output")
	}
	return nil
}
