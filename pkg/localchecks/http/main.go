package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/scorify/schema"
)

type Schema struct {
	URL            string `key:"url"`
	Verb           string `key:"verb" default:"GET" enum:"GET,POST,PUT,DELETE,PATCH,HEAD,OPTIONS,CONNECT,TRACE"`
	ExpectedOutput string `key:"expected_output"`
	MatchType      string `key:"match_type" default:"statusCode" enum:"statusCode,substringMatch,exactMatch,regexMatch"`
	Insecure       bool   `key:"insecure"`
	Headers        string `key:"headers"`
	Body           string `key:"body"`
	ContentType    string `key:"content_type" default:"empty" enum:"text/plain,application/json,application/x-www-form-urlencoded,empty"`
}

const maxResponseBytes = 1 << 20

var transports = [2]*http.Transport{
	{MaxIdleConns: 32, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second},
	{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, MaxIdleConns: 32, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second},
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.URL == "" {
		return fmt.Errorf("url must be provided; got: %v", conf.URL)
	}

	if !slices.Contains([]string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"}, conf.Verb) {
		return fmt.Errorf("invalid command provided: %v", conf.Verb)
	}

	if !slices.Contains([]string{"statusCode", "substringMatch", "exactMatch", "regexMatch"}, conf.MatchType) {
		return fmt.Errorf("invalid match type provided: %v", conf.MatchType)
	}

	if conf.ExpectedOutput == "" {
		return fmt.Errorf("expected_output must be provided; got: %v", conf.ExpectedOutput)
	}

	if conf.MatchType == "regexMatch" {
		if _, err := regexp.Compile(conf.ExpectedOutput); err != nil {
			return fmt.Errorf("invalid regex pattern provided: %v; %q", conf.ExpectedOutput, err)
		}
	}

	if conf.MatchType == "statusCode" {
		status_code, err := strconv.Atoi(conf.ExpectedOutput)
		if err != nil {
			return fmt.Errorf("invalid status code provided: %v; %q", conf.ExpectedOutput, err)
		}

		if status_code < 100 || status_code > 599 {
			return fmt.Errorf("invalid status code provided: %d", status_code)
		}
	}

	if conf.Headers != "" {
		for _, raw := range strings.Split(conf.Headers, ";") {
			if raw == "" {
				return fmt.Errorf("header format must be \"header:value;header:value\" ; got: %v", conf.Headers)
			}
			parts := strings.SplitN(raw, ":", 2)
			if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
				return fmt.Errorf("header format must be \"header:value;header:value\" ; got: %v", conf.Headers)
			}
		}
	}

	if conf.ContentType == "empty" && conf.Body != "" {
		return fmt.Errorf("body must not be provided when using empty Content-Type; got: %v", conf.Body)
	}

	if conf.ContentType != "empty" && conf.Body == "" {
		return fmt.Errorf("body must be provided when using non-empty Content-Type; got: %v", conf.Body)
	}

	if !slices.Contains([]string{"text/plain", "application/json", "application/x-www-form-urlencoded", "empty"}, conf.ContentType) {
		return fmt.Errorf("invalid content type provided: %v", conf.ContentType)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	var requestType string

	switch conf.Verb {
	case "GET":
		requestType = http.MethodGet
	case "POST":
		requestType = http.MethodPost
	case "PUT":
		requestType = http.MethodPut
	case "DELETE":
		requestType = http.MethodDelete
	case "PATCH":
		requestType = http.MethodPatch
	case "HEAD":
		requestType = http.MethodHead
	case "OPTIONS":
		requestType = http.MethodOptions
	case "CONNECT":
		requestType = http.MethodConnect
	case "TRACE":
		requestType = http.MethodTrace
	default:
		return fmt.Errorf("provided invalid command/http verb: %q", conf.Verb)
	}
	var req *http.Request
	if conf.ContentType == "empty" {
		req, err = http.NewRequestWithContext(ctx, requestType, conf.URL, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, requestType, conf.URL, bytes.NewBufferString(conf.Body))
	}
	if err != nil {
		return fmt.Errorf("encountered error while creating request: %v", err.Error())
	}

	if conf.ContentType != "empty" {
		req.Header.Set("Content-Type", conf.ContentType)
	}

	if conf.Headers != "" {
		for _, raw := range strings.Split(conf.Headers, ";") {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			parts := strings.SplitN(raw, ":", 2)
			if len(parts) != 2 {
				continue
			}
			key, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			if key != "" && value != "" {
				req.Header.Add(key, value)
			}
		}
	}

	transport := transports[0]
	if conf.Insecure {
		transport = transports[1]
	}
	client := &http.Client{Transport: transport}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("encountered error while making request: %v", err.Error())
	}
	defer resp.Body.Close()

	switch conf.MatchType {
	case "statusCode":
		status_code, err := strconv.Atoi(conf.ExpectedOutput)
		if err != nil {
			return fmt.Errorf("invalid status code provided: %v; %q", conf.ExpectedOutput, err)
		}

		if resp.StatusCode != status_code {
			return fmt.Errorf("expected status code: %d; got: %d", status_code, resp.StatusCode)
		}
	case "substringMatch":
		body, err := readBody(resp.Body)
		if err != nil {
			return fmt.Errorf("encountered error while reading response body: %v", err)
		}

		if !bytes.Contains(body, []byte(conf.ExpectedOutput)) {
			return fmt.Errorf("expected output not found in response body")
		}
	case "exactMatch":
		body, err := readBody(resp.Body)
		if err != nil {
			return fmt.Errorf("encountered error while reading response body: %v", err)
		}

		if string(body) != conf.ExpectedOutput {
			return fmt.Errorf("expected output not found in response body")
		}
	case "regexMatch":
		pattern, err := regexp.Compile(conf.ExpectedOutput)
		if err != nil {
			return fmt.Errorf("invalid regex pattern provided: %v; %q", conf.ExpectedOutput, err)
		}

		body, err := readBody(resp.Body)
		if err != nil {
			return fmt.Errorf("encountered error while reading response body: %v", err)
		}

		if !pattern.Match(body) {
			return fmt.Errorf("expected output not found in response body")
		}
	default:
		return fmt.Errorf("invalid match type provided: %v", conf.MatchType)
	}

	return nil
}

func readBody(body io.Reader) ([]byte, error) {
	out, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(out) > maxResponseBytes {
		return nil, fmt.Errorf("HTTP response exceeds 1 MiB check limit")
	}
	return out, nil
}
