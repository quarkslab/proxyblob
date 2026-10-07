package main

import (
	"github.com/rs/zerolog/log"
)

// main is the entry point for the application.
func main() {
	// Set up logging
	configureLogging()

	// Configure and create the CLI app
	app = setupCLI()

	// Add all command handlers
	AddCommands(app)

	// Run the application and handle any errors
	if err := app.Run(); err != nil {
		log.Fatal().Msg(err.Error())
	}
}
