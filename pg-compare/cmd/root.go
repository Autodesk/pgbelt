package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var CurrentClientVersion = "dev-build"
var Verbose bool = false
var Logger zerolog.Logger
var returnStr strings.Builder
var IsBeltIntegrated bool = false
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

// func getLogLevel() zerolog.Level {
// 	if Verbose {
// 		return -1
// 	}
// 	return 1
// }

// func ConfigureLogger() {
// 	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).
// 		Level(getLogLevel()).
// 		With().
// 		Timestamp().
// 		Logger()
// 	Logger = logger
// }

//	func init() {
//		// fmt.Println(os.Args)
//		// if len(os.Args) > 1 {
//		// 	os.Args = os.Args[1:]
//		// }
//		// fmt.Println(os.Args)
//		Logger = log.Logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
//		rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "verbose output")
//		cobra.OnInitialize(ConfigureLogger)
//	}
func ConfigurePgbelt(fileLocation string) string {
	IsBeltIntegrated = true
	Logger = log.Logger.Output(zerolog.ConsoleWriter{Out: &returnStr}).Level(zerolog.InfoLevel)
	output := CompareCommand(fileLocation)
	return output
}
func Execute(filePath string) {
	if err := rootCmd.Execute(); err != nil {
		_, err := fmt.Fprintln(os.Stderr, err)
		if err != nil {
			return
		}
		os.Exit(1)
	}
}
