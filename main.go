package main

import (
	"os"

	"github.com/scorify/scorify/pkg/cmd"
)

//go:generate go run ./tools/checkgen

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
