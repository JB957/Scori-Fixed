package dns

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/scorify/schema"
)

type Schema struct {
	Server         string `key:"dns_server"`
	Port           int    `key:"port" default:"53"`
	Record         string `key:"record" default:"A" enum:"A,AAAA,CNAME,MX,NS,PTR,TXT"`
	Domain         string `key:"domain"`
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

	if conf.Port == 0 {
		return fmt.Errorf("port is required; got %d", conf.Port)
	}

	if conf.Port < 1 || conf.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535; got %d", conf.Port)
	}

	if conf.Record == "" {
		return fmt.Errorf("record is required; got %q", conf.Record)
	}

	if !slices.Contains([]string{"A", "AAAA", "CNAME", "MX", "NS", "PTR", "TXT"}, conf.Record) {
		return fmt.Errorf("record must be one of A, AAAA, CNAME, MX, NS, PTR, TXT; got %q", conf.Record)
	}

	if conf.Domain == "" {
		return fmt.Errorf("domain is required; got %q", conf.Domain)
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

	connStr := fmt.Sprintf("%s:%d", conf.Server, conf.Port)

	r := new(net.Resolver)
	r.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return nil, fmt.Errorf("deadline not set")
		}

		d := net.Dialer{
			Deadline: deadline,
		}

		return d.DialContext(ctx, network, connStr)
	}

	var addresses []string

	switch conf.Record {
	case "A":
		ips, err := r.LookupIP(ctx, "ip4", conf.Domain)
		if err != nil {
			return err
		}

		addresses = make([]string, len(ips))
		for i, ip := range ips {
			addresses[i] = ip.String()
		}
	case "AAAA":
		ips, err := r.LookupIP(ctx, "ip6", conf.Domain)
		if err != nil {
			return err
		}

		addresses = make([]string, len(ips))
		for i, ip := range ips {
			addresses[i] = ip.String()
		}
	case "CNAME":
		cname, err := r.LookupCNAME(ctx, conf.Domain)
		if err != nil {
			return err
		}

		addresses = []string{cname}
	case "MX":
		mxs, err := r.LookupMX(ctx, conf.Domain)
		if err != nil {
			return err
		}

		addresses = make([]string, len(mxs))
		for i, mx := range mxs {
			addresses[i] = mx.Host
		}
	case "NS":
		nss, err := r.LookupNS(ctx, conf.Domain)
		if err != nil {
			return err
		}

		addresses = make([]string, len(nss))
		for i, ns := range nss {
			addresses[i] = ns.Host
		}
	case "PTR":
		ptrs, err := r.LookupAddr(ctx, conf.Domain)
		if err != nil {
			return err
		}

		addresses = make([]string, len(ptrs))
		copy(addresses, ptrs)
	case "TXT":
		txts, err := r.LookupTXT(ctx, conf.Domain)
		if err != nil {
			return err
		}

		addresses = make([]string, len(txts))
		copy(addresses, txts)
	default:
		return fmt.Errorf("unsupported record type: %q", conf.Record)
	}

	for _, address := range addresses {
		if address == conf.ExpectedOutput {
			return nil
		}
	}

	return fmt.Errorf("expected out %q not found in [%s]", conf.ExpectedOutput, strings.Join(addresses, ", "))
}
