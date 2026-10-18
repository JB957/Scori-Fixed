package check_template

import (
	"context"

	"github.com/scorify/schema"
)

// Schema is a custom defined struct that will hold the check configuration
type Schema struct {
	Target string `key:"target"` // Make sure to use the scorify tags to define the key in the config
	Port   int    `key:"port"`

	// Add any additional fields that you want to pass in as config
}

func Validate(config string) error {
	// Define a new Schema
	conf := Schema{}

	// Unmarshal the config to the Schema
	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	// Custom validation logic

	return nil
}

// Run is the function that will get called to run an instance of a check
func Run(ctx context.Context, config string) error {
	// Define a new Schema
	conf := Schema{}

	// Unmarshal the config to the Schema
	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	// Custom logic to run the check

	return nil
}
