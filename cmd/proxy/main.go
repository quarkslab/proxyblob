package main

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
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

// configureLogging sets up zerolog with appropriate formatting and level.
func configureLogging() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "15:04:05",
	})

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}
