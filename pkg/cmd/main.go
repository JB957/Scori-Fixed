package cmd

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/scorify/scorify/pkg/cmd/koth"
	"github.com/scorify/scorify/pkg/cmd/minion"
	"github.com/scorify/scorify/pkg/cmd/server"
	"github.com/scorify/scorify/pkg/cmd/setup"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "scorify",
	Short: "Scorify is a scoring engine for Cybersecurity Competitions",
	Long:  "Scorify is a scoring engine for Cybersecurity Competitions",
	Run:   run,
}

// print help if no subcommand is given
func run(cmd *cobra.Command, args []string) {
	err := cmd.Help()
	if err != nil {
		logrus.WithError(err).Fatal("failed to print help")
	}
}

// Entrypoint for all commands
func Execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownProfile, err := startProfileServer()
	if err != nil {
		return err
	}
	defer shutdownProfile()
	return rootCmd.ExecuteContext(ctx)
}

func startProfileServer() (func(), error) {
	address := os.Getenv("SCORIFY_PPROF_ADDR")
	if address == "" {
		return func() {}, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	for _, name := range []string{"heap", "goroutine", "allocs", "threadcreate", "block", "mutex"} {
		mux.Handle("/debug/pprof/"+name, pprof.Handler(name))
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logrus.WithError(err).Error("profile server stopped")
		}
	}()
	logrus.WithField("address", listener.Addr()).Info("profile server enabled")
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}, nil
}

// registers all commands
func init() {
	rootCmd.AddCommand(
		minion.Cmd,
		server.Cmd,
		setup.Cmd,
		koth.Cmd,
	)
}
