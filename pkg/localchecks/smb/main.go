package smb

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"regexp"
	"slices"
	"strings"

	"github.com/hirochachacha/go-smb2"
	"github.com/scorify/schema"
)

type Schema struct {
	Server         string `key:"server"`
	Port           int    `key:"port" default:"445"`
	Username       string `key:"username"`
	Password       string `key:"password"`
	Domain         string `key:"domain"`
	Share          string `key:"share" default:"C$"`
	File           string `key:"file"`
	MatchType      string `key:"match_type" default:"exists" enum:"exists,exactMatch,substringMatch,regexMatch,sha256,md5,sha1"`
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
		return fmt.Errorf("port must be valid; got %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username is required; got %q", conf.Username)
	}

	if conf.Domain == "" {
		return fmt.Errorf("domain is required; got %q", conf.Domain)
	}

	if conf.Share == "" {
		return fmt.Errorf("share is required; got %q", conf.Share)
	}

	if conf.File == "" {
		return fmt.Errorf("file is required; got %q", conf.File)
	}

	if !slices.Contains([]string{"exists", "exactMatch", "substringMatch", "regexMatch", "sha256", "md5", "sha1"}, conf.MatchType) {
		return fmt.Errorf("match_type must be one of exists, exactMatch, substringMatch, regexMatch, sha256, md5, sha1; got %q", conf.MatchType)
	}

	if conf.ExpectedOutput == "" && conf.MatchType != "exists" {
		return fmt.Errorf("expected_output is required for non-exist checks; got %q for %q", conf.ExpectedOutput, conf.MatchType)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	connStr := fmt.Sprintf("%s:%d", conf.Server, conf.Port)
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to %q; %w", connStr, err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return err
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	smbDialer := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     conf.Username,
			Password: conf.Password,
			Domain:   conf.Domain,
		},
	}

	smbConn, err := smbDialer.DialContext(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to dial %s: %w", connStr, err)
	}
	defer smbConn.Logoff()

	sharePath := fmt.Sprintf(`\\%s\%s`, conf.Server, conf.Share)
	fs, err := smbConn.Mount(sharePath)
	if err != nil {
		return fmt.Errorf("failed to mount %s: %w", sharePath, err)
	}
	defer fs.Umount()

	file, err := fs.Open(conf.File)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", conf.File, err)
	}
	defer file.Close()
	if conf.MatchType == "exists" {
		return nil
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to start of %s: %w", conf.File, err)
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", conf.File, err)
	}
	if len(bodyBytes) > 1<<20 {
		return fmt.Errorf("SMB file exceeds 1 MiB check limit")
	}

	switch conf.MatchType {
	case "exists":
		return nil
	case "exactMatch":
		if string(bodyBytes) != conf.ExpectedOutput {
			return fmt.Errorf("response does not match expected output: %q", conf.ExpectedOutput)
		}
	case "substringMatch":
		if !strings.Contains(string(bodyBytes), conf.ExpectedOutput) {
			return fmt.Errorf("response does not contain expected output: %q", conf.ExpectedOutput)
		}
	case "regexMatch":
		pattern, err := regexp.Compile(conf.ExpectedOutput)
		if err != nil {
			return fmt.Errorf("encountered error compiling regex pattern: %v", err)
		}

		if !pattern.Match(bodyBytes) {
			return fmt.Errorf("response does not match regex pattern: %s", conf.ExpectedOutput)
		}
	case "sha256":
		sha256 := fmt.Sprintf("%x", sha256.Sum256(bodyBytes))
		if sha256 != conf.ExpectedOutput {
			return fmt.Errorf("response does not match expected sha256: %s", conf.ExpectedOutput)
		}
	case "md5":
		md5 := fmt.Sprintf("%x", md5.Sum(bodyBytes))
		if md5 != conf.ExpectedOutput {
			return fmt.Errorf("response does not match expected md5: %s", conf.ExpectedOutput)
		}
	case "sha1":
		sha1 := fmt.Sprintf("%x", sha1.Sum(bodyBytes))
		if sha1 != conf.ExpectedOutput {
			return fmt.Errorf("response does not match expected sha1: %s", conf.ExpectedOutput)
		}
	default:
		return fmt.Errorf("unknown match type: %s", conf.MatchType)
	}

	return nil
}
