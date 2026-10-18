package imap

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/emersion/go-imap/client"
	"github.com/scorify/schema"
)

type Schema struct {
	Server   string `key:"server"`
	Port     int    `key:"port" default:"143"`
	Username string `key:"username"`
	Password string `key:"password"`
	Mailbox  string `key:"mailbox" default:"INBOX"`
	Secure   bool   `key:"secure"`
}

type existingConnDialer struct{ net.Conn }

func (d existingConnDialer) Dial(network, address string) (net.Conn, error) {
	return d.Conn, nil
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.Server == "" {
		return fmt.Errorf("target is required; got %q", conf.Server)
	}

	if conf.Port <= 0 || conf.Port > 65535 {
		return fmt.Errorf("provided invalid port: %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username is required; got %q", conf.Username)
	}

	if conf.Password == "" {
		return fmt.Errorf("password is required; got %q", conf.Password)
	}

	if conf.Mailbox == "" {
		return fmt.Errorf("mailbox is required; got %q", conf.Mailbox)
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
		return fmt.Errorf("context deadline is not set")
	}

	connStr := net.JoinHostPort(conf.Server, fmt.Sprint(conf.Port))
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", connStr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	var imapClient *client.Client
	if conf.Secure {
		imapClient, err = client.DialWithDialerTLS(existingConnDialer{conn}, connStr, &tls.Config{ServerName: conf.Server, InsecureSkipVerify: true})
	} else {
		imapClient, err = client.DialWithDialer(existingConnDialer{conn}, connStr)
	}
	if err != nil {
		return err
	}
	defer imapClient.Close()
	imapClient.Timeout = time.Until(deadline)

	err = imapClient.Login(conf.Username, conf.Password)
	if err != nil {
		return fmt.Errorf("failed to login: %w", err)
	}

	_, err = imapClient.Select(conf.Mailbox, true)
	if err != nil {
		return fmt.Errorf("failed opening mailbox %q: %w", conf.Mailbox, err)
	}

	return nil
}
