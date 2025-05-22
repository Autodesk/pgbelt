package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/spf13/cobra"
)

var configFile string
var getFalseRecordsOnly bool
var Config PgConfig

func init() {
	compareCmd.Flags().StringVarP(&configFile, "config", "c", "", "config file")
	compareCmd.Flags().BoolVarP(&getFalseRecordsOnly, "show-false", "s", false, "show only tables with different row count or failed attempts")
	rootCmd.AddCommand(compareCmd)
}

func createConnection(conf DBConfig, suffix string) *PgConnection {
	pgConn := PgConnection{config: conf}
	password := url.QueryEscape(conf.RootUser.PW)
	connUrl := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", conf.RootUser.Name, password, conf.Host, conf.Port, conf.DB)
	err := pgConn.Connect(context.Background(), connUrl)
	if err != nil {
		Logger.Error().Err(err).Msgf("failed to connect: %s", connUrl)
		return nil
	}
	Logger.Info().Msgf("connected to %s", conf.Host)
	pgConn.SetSubLogger(suffix)
	return &pgConn
}

func createOwnerConnection(conf DBConfig, suffix string) *PgConnection {
	pgConn := PgConnection{config: conf}
	password := url.QueryEscape(conf.OwnerUser.PW)
	connUrl := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", conf.OwnerUser.Name, password, conf.Host, conf.Port, conf.DB)
	err := pgConn.Connect(context.Background(), connUrl)
	if err != nil {
		Logger.Error().Err(err).Msgf("failed to connect: %s", connUrl)
		return nil
	}
	Logger.Info().Msgf("connected to %s", conf.Host)
	pgConn.SetSubLogger(suffix)
	return &pgConn
}

func itemExists(array []Table, item string) bool {
	for _, element := range array {
		if element.Name == item {
			return true
		}
	}
	return false
}
func stringExists(array []string, item string) bool {
	for _, element := range array {
		if element == item {
			return true
		}
	}
	return false
}
func indexExists(array []Index, item string) bool {
	for _, element := range array {
		if element.Index == item {
			return true
		}
	}
	return false
}
func renderConnections(connections []Connection) {
	connTable := table.NewWriter()
	connTable.SetOutputMirror(os.Stdout)
	connTable.AppendHeader(table.Row{"Pid", "Username", "DBname", "ClientAdders", "Status", "Query"})
	for _, connection := range connections {
		connTable.AppendRow([]interface{}{connection.Pid, connection.Username, connection.DBname, connection.ClientAdders, connection.Status, connection.Query})
		connTable.AppendSeparator()
	}
	connTable.Render()
}
func sequenceExists(array []Sequence, item string) bool {
	for _, element := range array {
		if element.Name == item {
			return true
		}
	}
	return false
}

var compareCmd = &cobra.Command{
	Use:   "compare",
	Short: "compares dbs",
	Run: func(cmd *cobra.Command, args []string) {
		t := table.NewWriter()
		i := table.NewWriter()
		s := table.NewWriter()
		seq := table.NewWriter()

		t.SetOutputMirror(os.Stdout)
		s.SetOutputMirror(os.Stdout)
		i.SetOutputMirror(os.Stdout)
		seq.SetOutputMirror(os.Stdout)

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
		Logger.Info().Msg("config loaded")
		srcConn := createConnection(Config.Src, "SOURCE")
		dstConn := createConnection(Config.Dst, "DESTINATION")
		dstOwnerConn := createOwnerConnection(Config.Dst, "DESTINATION")
		t.AppendHeader(table.Row{"Table Name", fmt.Sprintf("Source: %s", srcConn.config.Host[:4]), fmt.Sprintf("Destination: %s", dstConn.config.Host[:4]), "Equal"})
		s.AppendHeader(table.Row{"Extra Comparison Items", fmt.Sprintf("Source: %s", srcConn.config.Host[:4]), fmt.Sprintf("Destination: %s", dstConn.config.Host[:4]), "Equal"})
		i.AppendHeader(table.Row{"Index Count", fmt.Sprintf("Source: %s", srcConn.config.Host[:4]), fmt.Sprintf("Destination: %s", dstConn.config.Host[:4]), "Equal"})
		seq.AppendHeader(table.Row{"Sequence Count", fmt.Sprintf("Source: %s", srcConn.config.Host[:4]), fmt.Sprintf("Destination: %s", dstConn.config.Host[:4]), "Equal"})

		defer func() { srcConn.CloseConn(context.Background()); dstConn.CloseConn(context.Background()) }()
		srcTables, err := srcConn.GetTables(context.Background())
		if err != nil {
			Logger.Error().Err(err).Msg("failed to get indexes")
			return
		}
		dstTables, err := dstConn.GetTables(context.Background())
		if err != nil {
			Logger.Error().Err(err).Msg("failed to get indexes")
			return
		}
		var notFoundTables int = 0
		for _, table := range srcTables {
			var tablesEquals bool = true
			t.AppendSeparator()
			if !itemExists(dstTables, table.Name) {
				notFoundTables++
				tablesEquals = false
				t.AppendRow([]interface{}{table, "", "NOTFOUND", tablesEquals})
				t.AppendSeparator()
				Logger.Debug().Msgf("table %s does not exist in dst", table)
				continue
			}
			srcCount, err := srcConn.GetTableRowCount(context.Background(), table)
			if err != nil {
				tablesEquals = false
				Logger.Error().Err(err).Msg("failed to get row count")
				t.AppendRow([]interface{}{table, "FAILED", "SRC FAIL", tablesEquals})
				t.AppendSeparator()
				continue
			}
			dstCount, err := dstConn.GetTableRowCount(context.Background(), table)
			if err != nil {
				Logger.Error().Err(err).Msg("failed to get row count")
				tablesEquals = false
				t.AppendRow([]interface{}{table, srcCount, "FAILED", tablesEquals})
				t.AppendSeparator()
				continue
			}
			if srcCount != dstCount {
				tablesEquals = false
				Logger.Debug().Msgf("table %s has different row count: %d vs %d", table, srcCount, dstCount)
			}
			if getFalseRecordsOnly && tablesEquals {
				continue
			}
			t.AppendRow([]interface{}{table, srcCount, dstCount, tablesEquals})
		}
		t.AppendSeparator()
		Logger.Info().Msgf("tables not found in dst: %d", notFoundTables)
		t.Render()

		srcIndexes, err := srcConn.GetIndexes(context.Background())
		if err != nil {
			Logger.Error().Err(err).Msg("failed to get indexes")
			return
		}
		dstIndexes, err := dstConn.GetIndexes(context.Background())
		if err != nil {
			Logger.Error().Err(err).Msg("failed to get indexes")
			return
		}
		indexesEquals := srcIndexes == dstIndexes
		i.AppendRow([]interface{}{"", srcIndexes, dstIndexes, indexesEquals})
		srcIndexesList, err := srcConn.GetIndexesList(context.Background())
		if err != nil {
			Logger.Error().Err(err).Msg("failed to get indexes list")
			return
		}
		dstIndexesList, err := dstConn.GetIndexesList(context.Background())
		if err != nil {
			Logger.Error().Err(err).Msg("failed to get indexes list")
			return
		}
		var notFoundIndexes int = 0
		for _, index := range srcIndexesList {
			var indexesEquals bool = true
			i.AppendSeparator()
			if !indexExists(dstIndexesList, index.Index) {
				notFoundIndexes++
				indexesEquals = false
				i.AppendRow([]interface{}{index, "FOUND", "NOTFOUND", indexesEquals})
				i.AppendSeparator()
				Logger.Debug().Msgf("index %s does not exist in dst", index.Index)
				generatedStr, genStrErr := srcConn.GetString(context.Background(), fmt.Sprintf("SELECT pg_get_indexdef('%s'::regclass);", index.Index), "index")
				if genStrErr != nil {
					Logger.Error().Err(genStrErr).Msg("failed to get index definition")
				} else {
					Logger.Debug().Msgf("index definition: %s", generatedStr)
				}
				continue
			}
			if getFalseRecordsOnly && indexesEquals {
				continue
			}
			i.AppendRow([]interface{}{index, "FOUND", "FOUND", indexesEquals})
		}
		i.Render()

		srcSequences, srcSeqErr := srcConn.GetSequences(context.Background())
		if srcSeqErr != nil {
			Logger.Error().Err(srcSeqErr).Msg("failed to get sequences")
			return
		}
		dstSequences, dstSeqErr := dstConn.GetSequences(context.Background())
		if dstSeqErr != nil {
			Logger.Error().Err(dstSeqErr).Msg("failed to get sequences")
			return
		}
		sequencesCountEquals := len(srcSequences) == len(dstSequences)

		Logger.Debug().Msgf("src sequences: %d, dst sequences: %d", len(srcSequences), len(dstSequences))
		seq.AppendRow([]interface{}{"", len(srcSequences), len(dstSequences), sequencesCountEquals})
		seq.AppendSeparator()
		missingSequences := []Sequence{}
		for _, sequence := range srcSequences {
			sequenceEquals := true
			if !sequenceExists(dstSequences, sequence.Name) {
				missingSequences = append(missingSequences, sequence)
				sequenceEquals = false
				seq.AppendRow([]interface{}{sequence, "", "NOTFOUND", sequenceEquals})
				seq.AppendSeparator()
				Logger.Debug().Msgf("sequence %s does not exist in dst", sequence)
				continue
			}
			if getFalseRecordsOnly && sequenceEquals {
				continue
			}
			seq.AppendRow([]interface{}{sequence, "FOUND", "FOUND", sequenceEquals})
			seq.AppendSeparator()
		}
		seq.Render()
		if len(missingSequences) != 0 && strings.Contains(configFile, "schedule") {
			createBool := true
			inputMissingSeq := bufio.NewScanner(os.Stdin)
			Logger.Info().Msg("Do you want to create missing sequences? !RELATED TO SCHEDULER-SERVICE! (yes/no)")
			inputMissingSeq.Scan()
			if inputMissingSeq.Text() != "yes" {
				createBool = false
				Logger.Info().Msg("skipping creating missing seqeuence")
			}
			if createBool {
				err = dstOwnerConn.CreateDiffSequences(missingSequences, context.Background())
				if err != nil {
					Logger.Error().Err(err).Msg("failed to create missing seqeucnes")
				}
			}
		}

		queries := []map[string]string{
			{"query": "select count(*) from pg_stat_user_tables where schemaname='public' and relname NOT LIKE '%dms%';", "name": "pg_stat_user_tables"},
			{"query": "select count(*) from pg_stat_user_indexes where schemaname='public' and relname NOT LIKE '%dms%';", "name": "pg_stat_user_indexes"},
			{"query": "select count(*) from pg_stat_user_functions where schemaname='public' and funcname NOT LIKE '%dms%';", "name": "pg_stat_user_functions"},
		}
		for _, query := range queries {
			srcCount, err := srcConn.GetCount(context.Background(), query["query"], query["name"])
			if err != nil {
				Logger.Error().Err(err).Msg("failed to get count")
				s.AppendRow([]interface{}{query, "FAILED", "SRC FAIL", false})
				s.AppendSeparator()
				continue
			}
			dstCount, err := dstConn.GetCount(context.Background(), query["query"], query["name"])
			if err != nil {
				Logger.Error().Err(err).Msg("failed to get count")
				s.AppendRow([]interface{}{query, srcCount, "FAILED", false})
				s.AppendSeparator()
				continue
			}
			s.AppendRow([]interface{}{query["name"], srcCount, dstCount, srcCount == dstCount})
		}
		s.Render()

		srcConns, srcConnsErr := srcConn.GetCurrentConnections(context.Background())
		if srcConnsErr != nil {
			Logger.Error().Err(srcConnsErr).Msg("failed to get connections")
		} else {
			renderConnections(srcConns)
		}
		dstConns, dstConnsErr := dstConn.GetCurrentConnections(context.Background())
		if dstConnsErr != nil {
			Logger.Error().Err(dstConnsErr).Msg("failed to get connections")
		} else {
			renderConnections(dstConns)
		}

	},
}
