package ftp

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"regexp"
	"slices"

	"github.com/jlaffaye/ftp"
	"github.com/scorify/schema"
)

type Schema struct {
	Target         string `key:"target"`
	Port           int    `key:"port" default:"21"`
	Username       string `key:"username" default:"anonymous"`
	Password       string `key:"password"`
	File           string `key:"file"`
	MatchType      string `key:"matchType" default:"exists" enum:"exists,substringMatch,regexMatch,exactMatch,sha256,md5,sha1"`
	ExpectedOutput string `key:"expectedOutput"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return fmt.Errorf("encountered error unmarshalling config: %v", err)
	}

	if conf.Target == "" {
		return fmt.Errorf("target must be provided; got: %s", conf.Target)
	}

	if conf.Port == 0 {
		return fmt.Errorf("port must be provided; got: %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username must be provided; got: %s", conf.Username)
	}

	if !slices.Contains([]string{"exists", "substringMatch", "regexMatch", "exactMatch", "sha256", "md5", "sha1"}, conf.MatchType) {
		return fmt.Errorf("matchType must be one of: exists, substringMatch, regexMatch, exactMatch, sha256, md5, sha1; got: %s", conf.MatchType)
	}

	if conf.ExpectedOutput == "" && conf.MatchType != "exists" {
		return fmt.Errorf("expectedOutput must be provided for all comparison types except exists; got: %s", conf.ExpectedOutput)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return fmt.Errorf("encountered error unmarshalling config: %v", err)
	}

	connStr := net.JoinHostPort(conf.Target, fmt.Sprint(conf.Port))

	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("context deadline is not set")
	}
	conn, err := ftp.Dial(connStr, ftp.DialWithDialFunc(func(network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if err := conn.SetDeadline(deadline); err != nil {
			conn.Close()
			return nil, err
		}
		return conn, nil
	}))
	if err != nil {
		return fmt.Errorf("encountered error creating ftp connection: %v", err)
	}
	defer conn.Quit()

	err = login(ctx, conn, conf.Username, conf.Password)
	if err != nil {
		return fmt.Errorf("encountered error logging in: %v", err)
	}
	defer conn.Logout()

	resp, err := conn.Retr(conf.File)
	if err != nil {
		return fmt.Errorf("encountered error retrieving file: %v", err)
	}
	defer resp.Close()
	if conf.MatchType == "exists" {
		return nil
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(resp, 1<<20+1))
	if err != nil {
		return fmt.Errorf("encountered error reading response body: %v", err)
	}
	if len(bodyBytes) > 1<<20 {
		return fmt.Errorf("ftp file exceeds 1 MiB check limit")
	}

	switch conf.MatchType {
	case "exists":
		return nil
	case "substringMatch":
		if bytes.Contains(bodyBytes, []byte(conf.ExpectedOutput)) {
			return nil
		}
		return fmt.Errorf("response does not match substring: %s", conf.ExpectedOutput)
	case "regexMatch":
		pattern, err := regexp.Compile(conf.ExpectedOutput)
		if err != nil {
			return fmt.Errorf("encountered error compiling regex pattern: %v", err)
		}

		if pattern.Match(bodyBytes) {
			return nil
		}
		return fmt.Errorf("response does not match regex pattern: %s", conf.ExpectedOutput)
	case "exactMatch":
		if string(bodyBytes) == conf.ExpectedOutput {
			return nil
		}
		return fmt.Errorf("response does not match expected output")
	case "sha256":
		sha256 := fmt.Sprintf("%x", sha256.Sum256(bodyBytes))
		if sha256 == conf.ExpectedOutput {
			return nil
		}
		return fmt.Errorf("response does not match expected sha256: %s", conf.ExpectedOutput)
	case "md5":
		md5 := fmt.Sprintf("%x", md5.Sum(bodyBytes))
		if md5 == conf.ExpectedOutput {
			return nil
		}
		return fmt.Errorf("response does not match expected md5: %s", conf.ExpectedOutput)
	case "sha1":
		sha1 := fmt.Sprintf("%x", sha1.Sum(bodyBytes))
		if sha1 == conf.ExpectedOutput {
			return nil
		}
		return fmt.Errorf("response does not match expected sha1: %s; got %s", conf.ExpectedOutput, sha1)
	default:
		return fmt.Errorf("unknown match type: %s", conf.MatchType)
	}
}

func login(ctx context.Context, conn *ftp.ServerConn, username string, password string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return conn.Login(username, password)
}
