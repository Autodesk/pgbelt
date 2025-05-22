package cmd

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var CurrentClientVersion = "dev-build"
var Verbose bool
var Logger zerolog.Logger

var rootCmd = &cobra.Command{
	Use:   "pg-compare",
	Short: "pg-compare",
	Long:  "pg compare tables.",
	Run: func(cmd *cobra.Command, args []string) {
		errHelp := cmd.Help()
		if errHelp != nil {
			return
		}
	},
}

func getLogLevel() zerolog.Level {
	if Verbose {
		return -1
	}
	return 1
}

func ConfigureLogger() {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).
		Level(getLogLevel()).
		With().
		Timestamp().
		Logger()
	Logger = logger
}
func init() {
	Logger = log.Logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "verbose output")
	cobra.OnInitialize(ConfigureLogger)
}
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, err := fmt.Fprintln(os.Stderr, err)
		if err != nil {
			return
		}
		os.Exit(1)
	}
}
