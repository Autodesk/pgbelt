package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/sethvargo/go-password/password"
	"github.com/spf13/cobra"
)

var prepareStatement string = ``
var prepareFunctionsStatement string = ``
var truncateDestination bool

func init() {
	prepareCmd.Flags().StringVarP(&configFile, "config", "c", "", "config file")
	prepareCmd.Flags().BoolVarP(&truncateDestination, "truncateDestination", "t", false, "truncate destination tables")
	// compareCmd.Flags().BoolVarP(&getFalseRecordsOnly, "show-false", "s", false, "show only tables with different row count or failed attempts")
	rootCmd.AddCommand(prepareCmd)
}
func GeneratePassword() string {
	res, err := password.Generate(32, 10, 0, false, false)
	if err != nil {
		Logger.Error().Err(err).Msg("failed to generate password")
		return ""
	}
	return res
}

func AddTables(source *PgConnection) {
	requiredOwner := source.config.OwnerUser.Name
	unknownUsers := map[string]map[string][]string{}
	srcTables, err := source.GetTables(context.Background())
	if err != nil {
		Logger.Error().Err(err).Msg("failed to get tables")
		return
	}
	for _, table := range srcTables {
		if table.Owner == "rdsadmin" {
			continue
		}
		if table.Owner != requiredOwner {
			if _, ok := unknownUsers[table.Owner]; !ok {
				unknownUsers[table.Owner] = map[string][]string{}
			}
			if _, ok := unknownUsers[table.Owner][table.Scheme]; !ok {
				unknownUsers[table.Owner][table.Scheme] = []string{}
			}
			unknownUsers[table.Owner][table.Scheme] = append(unknownUsers[table.Owner][table.Scheme], table.Name)
		}
	}
	prepareStatement = prepareStatement + `
set local lock_timeout='2s';`
	for user, schemes := range unknownUsers {
		Logger.Info().Msgf("%s, tables: %s", user, schemes)
		for scheme, tables := range schemes {
			prepareStatement = prepareStatement + fmt.Sprintf(`

GRANT ALL ON SCHEMA %s TO %s;`, scheme, requiredOwner)
			for _, table := range tables {
				prepareStatement = prepareStatement + fmt.Sprintf(`
ALTER TABLE %s."%s" OWNER TO %s;`, scheme, table, requiredOwner)
				prepareStatement = prepareStatement + fmt.Sprintf(`
GRANT ALL ON %s."%s" TO %s;`, scheme, table, user)
			}
		}

	}
}

func AddSequences(source *PgConnection) {
	requiredOwner := source.config.OwnerUser.Name
	unknownUsers := map[string]map[string][]string{}
	// handle tables
	srcSequences, err := source.GetSequences(context.Background())
	if err != nil {
		Logger.Error().Err(err).Msg("failed to get sequences")
		return
	}
	for _, sequence := range srcSequences {
		if sequence.Owner == "rdsadmin" || sequence.Owner == "rds_superuser" {
			continue
		}
		// Logger.Info().Msgf("source sequence: %v", sequence)
		if sequence.Owner != requiredOwner {
			if _, ok := unknownUsers[sequence.Owner]; !ok {
				unknownUsers[sequence.Owner] = map[string][]string{}
			}
			if _, ok := unknownUsers[sequence.Owner][sequence.Scheme]; !ok {
				unknownUsers[sequence.Owner][sequence.Scheme] = []string{}
			}
			unknownUsers[sequence.Owner][sequence.Scheme] = append(unknownUsers[sequence.Owner][sequence.Scheme], sequence.Name)
		}
	}
	for user, schemes := range unknownUsers {
		Logger.Info().Msgf("%s, sequences: %s", user, schemes)
		for scheme, sequences := range schemes {

			for _, sequence := range sequences {
				prepareStatement = prepareStatement + fmt.Sprintf(`
ALTER SEQUENCE %s."%s" OWNER TO %s;`, scheme, sequence, requiredOwner)
			}
			prepareStatement = prepareStatement + fmt.Sprintf(`
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s TO %s;`, scheme, user)
		}
	}
}
func AddViews(source *PgConnection) {
	requiredOwner := source.config.OwnerUser.Name
	unknownUsers := map[string]map[string][]string{}
	// handle tables
	srcSequences, err := source.GetViews(context.Background())
	if err != nil {
		Logger.Error().Err(err).Msg("failed to get views")
		return
	}
	for _, sequence := range srcSequences {
		if sequence.Owner == "rdsadmin" {
			continue
		}
		// Logger.Info().Msgf("source sequence: %v", sequence)
		if sequence.Owner != requiredOwner {
			if _, ok := unknownUsers[sequence.Owner]; !ok {
				unknownUsers[sequence.Owner] = map[string][]string{}
			}
			if _, ok := unknownUsers[sequence.Owner][sequence.Scheme]; !ok {
				unknownUsers[sequence.Owner][sequence.Scheme] = []string{}
			}
			unknownUsers[sequence.Owner][sequence.Scheme] = append(unknownUsers[sequence.Owner][sequence.Scheme], sequence.Name)
		}
	}
	for user, schemes := range unknownUsers {
		Logger.Debug().Msgf("%s, views: %s", user, schemes)
		for scheme, sequences := range schemes {
			prepareStatement = prepareStatement + `
`
			for _, sequence := range sequences {
				prepareStatement = prepareStatement + fmt.Sprintf(`
ALTER VIEW %s."%s" OWNER TO %s;`, scheme, sequence, requiredOwner)
				prepareStatement = prepareStatement + fmt.Sprintf(`
GRANT SELECT ON %s."%s" TO %s;`, scheme, sequence, user)
			}
		}
	}
}
func AddFunctions(source *PgConnection) {
	requiredOwner := source.config.OwnerUser.Name
	unknownUsers := map[string]map[string][]string{}
	// handle tables
	srcSequences, err := source.GetFunctions(context.Background())
	if err != nil {
		Logger.Error().Err(err).Msg("failed to get functions")
		return
	}
	for _, sequence := range srcSequences {
		// Logger.Info().Msgf("source sequence: %v", sequence)
		if sequence.Owner == "rdsadmin" || sequence.Owner == "rds_superuser" {
			continue
		}
		if sequence.Owner != requiredOwner {
			if _, ok := unknownUsers[sequence.Owner]; !ok {
				unknownUsers[sequence.Owner] = map[string][]string{}
			}
			if _, ok := unknownUsers[sequence.Owner][sequence.Scheme]; !ok {
				unknownUsers[sequence.Owner][sequence.Scheme] = []string{}
			}
			unknownUsers[sequence.Owner][sequence.Scheme] = append(unknownUsers[sequence.Owner][sequence.Scheme], fmt.Sprintf("%s|%s", sequence.Name, sequence.Arguments))
		}
	}
	prepareFunctionsStatement = prepareFunctionsStatement + `
set local lock_timeout='2s';`
	for user, schemes := range unknownUsers {
		Logger.Debug().Msgf("%s, functions: %s", user, schemes)
		for scheme, sequences := range schemes {
			prepareFunctionsStatement = prepareFunctionsStatement + `
`
			for _, sequence := range sequences {
				name := strings.Split(sequence, "|")[0]
				arguments := strings.Split(sequence, "|")[1]
				prepareFunctionsStatement = prepareFunctionsStatement + fmt.Sprintf(`
ALTER FUNCTION %s."%s"(%s) OWNER TO %s;`, scheme, name, arguments, requiredOwner)
				prepareFunctionsStatement = prepareFunctionsStatement + fmt.Sprintf(`
GRANT EXECUTE ON FUNCTION %s."%s"(%s) TO %s;`, scheme, name, arguments, user)
			}
		}
	}
}

func checkDestinationOwnerShip(ctx context.Context, conn *PgConnection) error {
	defer func() { conn.CloseConn(context.Background()) }()
	requiredOwner := conn.config.OwnerUser.Name
	dstTables, err := conn.GetTables(context.Background())
	if err != nil {
		Logger.Error().Err(err).Msg("failed to get tables")
		return err
	}
	for _, table := range dstTables {
		if table.Owner == "rdsadmin" || table.Owner == "rds_superuser" {
			continue
		}
		if table.Owner != requiredOwner {
			Logger.Warn().Msgf("table %s.%s owner is %s instead of %s", table.Scheme, table.Name, table.Owner, requiredOwner)
		}
	}
	return nil
}

var prepareCmd = &cobra.Command{
	Use:   "prepare",
	Short: "prepare source db and target db",
	Run: func(cmd *cobra.Command, args []string) {
		file, err := os.ReadFile(configFile)
		if err != nil {
			Logger.Error().Err(err).Msg("failed to open config file")
			return
		}
		err = json.Unmarshal(file, &Config)
		if err != nil {
			Logger.Error().Err(err).Msg("failed to unmarshal config file")
			return
		}
		requiredOwner := Config.Src.OwnerUser.Name
		Logger.Info().Msg("config loaded")
		srcConn, connErr := createConnection(Config.Src, "SOURCE")
		if connErr != nil {
			Logger.Error().Err(connErr).Msg("failed to create source connection")
			return
		}
		defer func() { srcConn.CloseConn(context.Background()) }()
		// Check Required user exists
		count, err := srcConn.GetCount(context.Background(), fmt.Sprintf("SELECT count(*) FROM pg_roles WHERE rolname = '%s';", requiredOwner), requiredOwner)
		if err != nil {
			Logger.Error().Err(err).Msg("failed to get count")
			return
		}
		if count == 0 {
			Logger.Error().Msgf("required owner %s does not exist", requiredOwner)
			prepareStatement = fmt.Sprintf(`
CREATE USER %s WITH PASSWORD '%s';
GRANT ALL PRIVILEGES ON DATABASE %s TO %s;
GRANT ALL ON SCHEMA pglogical TO %s;
			`, requiredOwner, GeneratePassword(), Config.Src.DB, requiredOwner, requiredOwner)
		}

		AddTables(srcConn)
		AddSequences(srcConn)
		AddViews(srcConn)
		AddFunctions(srcConn)
		Logger.Debug().Msgf("prepare functions statement: \n%s\n", prepareFunctionsStatement)
		Logger.Debug().Msgf("prepare statement: \n%s\n", prepareStatement)
		// execute prepare statement
		input := bufio.NewScanner(os.Stdin)
		Logger.Info().Msgf("Owner changes will be applied to source db: %s", Config.Src.DB)
		Logger.Info().Msg("Do you want to apply ownership changes? (yes/no)")
		input.Scan()
		if input.Text() == "yes" {
			err = srcConn.ExecuteTransaction(context.Background(), prepareFunctionsStatement)
			if err != nil {
				Logger.Error().Err(err).Msg("failed to execute functions prepare statement")
				return
			}
			err = srcConn.ExecuteTransaction(context.Background(), prepareStatement)
			if err != nil {
				Logger.Error().Err(err).Msg("failed to execute prepare statement")
				return
			}
		} else {
			Logger.Info().Msg("skipping ownership changes")
		}
		if truncateDestination {
			dstConn, connErr := createConnection(Config.Dst, "DESTINATION")
			if connErr != nil {
				Logger.Error().Err(connErr).Msg("failed to create destination connection")
				return
			}
			defer func() { dstConn.CloseConn(context.Background()) }()
			inputTrun := bufio.NewScanner(os.Stdin)
			Logger.Info().Msgf("Truncate Tables will be applied to destination db: %s", Config.Dst.DB)
			dstTables, dstTablesErr := dstConn.GetTables(context.Background())
			if dstTablesErr != nil {
				Logger.Error().Err(dstTablesErr).Msg("failed to get tables")
				return
			}
			dstTablesFiltered := []Table{}
			if len(Config.Tables) != 0 {
				for _, table := range dstTables {
					if stringExists(Config.Tables, table.Name) {
						dstTablesFiltered = append(dstTablesFiltered, table)
					}
				}
			} else {
				dstTablesFiltered = append(dstTablesFiltered, dstTables...)
			}
			for _, table := range dstTablesFiltered {
				Logger.Info().Msgf("Table: %s.%s", table.Scheme, table.Name)
			}
			Logger.Info().Msg("Do you want to truncate tables? (yes/no)")
			inputTrun.Scan()
			if inputTrun.Text() != "yes" {
				Logger.Info().Msg("skipping truncate tables")
				return
			}
			err = dstConn.TruncateTables(dstTablesFiltered, context.Background())
			if err != nil {
				Logger.Error().Err(err).Msg("failed to truncate tables")
				return
			}
		}
		dstConn, connErr := createConnection(Config.Dst, "DESTINATION")
		if connErr != nil {
			Logger.Error().Err(connErr).Msg("failed to create destination connection")
			return
		}
		checkDestinationOwnerShip(context.Background(), dstConn)
	},
}
