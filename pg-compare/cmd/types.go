package cmd

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
)

type PgConfig struct {
	DB        string   `json:"db"`
	DC        string   `json:"dc"`
	Src       DBConfig `json:"src"`
	Dst       DBConfig `json:"dst"`
	Tables    []string `json:"tables"`
	Sequences []string `json:"sequences"`
}

type DBConfig struct {
	Host          string      `json:"host"`
	IP            string      `json:"ip"`
	DB            string      `json:"db"`
	Port          string      `json:"port"`
	RootUser      User        `json:"root_user"`
	PGLogicalUser User        `json:"pglogical_user"`
	OtherUsers    interface{} `json:"other_users"`
	OwnerUser     *User       `json:"owner_user,omitempty"`
}

type User struct {
	Name string `json:"name"`
	PW   string `json:"pw"`
}
type Table struct {
	Name   string
	Scheme string
	Owner  string
}
type Sequence struct {
	Name   string
	Scheme string
	Owner  string
}
type Function struct {
	Name      string
	Scheme    string
	Owner     string
	Arguments string
}
type PgConnection struct {
	conn   pgx.Conn
	config DBConfig
	log    zerolog.Logger
}

func (pc *PgConnection) SetSubLogger(suffix string) {
	pc.log = Logger.With().Str("server", suffix).Logger()
}

func (pc *PgConnection) Connect(ctx context.Context, connString string) error {
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		return err
	}
	pc.conn = *conn
	return nil
}

func (pc *PgConnection) CloseConn(ctx context.Context) error {
	err := pc.conn.Close(ctx)
	if err != nil {
		return err
	}
	return nil
}

type Index struct {
	Id    int
	Table string
	Index string
}
type Connection struct {
	Pid          string
	Username     string
	DBname       string
	ClientAdders string
	Status       string
	Query        string
}

func (pc *PgConnection) GetTables(ctx context.Context) ([]Table, error) {
	pc.log.Info().Msg("getting tables")
	var tables []Table
	result, err := pc.conn.Query(ctx, "SELECT schemaname, tablename, tableowner FROM pg_tables where schemaname not in ('information_schema', 'pg_catalog', 'pglogical') AND tablename != 'spatial_ref_sys';")
	if err != nil {
		return tables, err
	}
	defer result.Close()
	for result.Next() {
		var scheme string
		var table string
		var tableOwner string
		err := result.Scan(&scheme, &table, &tableOwner)
		if err != nil {
			pc.log.Error().Err(err)
		}
		// pc.log.Debug().Msgf("table: %v", table)
		tables = append(tables, Table{Name: table, Scheme: scheme, Owner: tableOwner})
	}
	if result.Err() != nil {
		return tables, err
	}
	return tables, nil
}
func (pc *PgConnection) GetSequences(ctx context.Context) ([]Sequence, error) {
	pc.log.Info().Msg("getting sequences")
	var sequences []Sequence
	result, err := pc.conn.Query(ctx, "SELECT n.nspname AS schema_name, c.relname AS sequence_name ,r.rolname AS owner FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_roles r ON r.oid = c.relowner WHERE c.relkind = 'S' ORDER BY schema_name, sequence_name;")
	if err != nil {
		return sequences, err
	}
	defer result.Close()
	for result.Next() {
		var scheme string
		var name string
		var sequenceOwner string
		err := result.Scan(&scheme, &name, &sequenceOwner)
		if err != nil {
			pc.log.Error().Err(err)
		}
		sequences = append(sequences, Sequence{Name: name, Scheme: scheme, Owner: sequenceOwner})
	}
	if result.Err() != nil {
		return sequences, err
	}
	return sequences, nil
}

func (pc *PgConnection) GetViews(ctx context.Context) ([]Sequence, error) {
	pc.log.Info().Msg("getting views")
	var sequences []Sequence
	result, err := pc.conn.Query(ctx, "SELECT schemaname, viewname, viewowner FROM pg_catalog.pg_views where schemaname NOT IN ('pg_catalog', 'repack', 'information_schema');")
	if err != nil {
		return sequences, err
	}
	defer result.Close()
	for result.Next() {
		var scheme string
		var name string
		var sequenceOwner string
		err := result.Scan(&scheme, &name, &sequenceOwner)
		if err != nil {
			pc.log.Error().Err(err)
		}
		sequences = append(sequences, Sequence{Name: name, Scheme: scheme, Owner: sequenceOwner})
	}
	if result.Err() != nil {
		return sequences, err
	}
	return sequences, nil
}
func (pc *PgConnection) GetFunctions(ctx context.Context) ([]Function, error) {
	pc.log.Info().Msg("getting functions")
	var functions []Function
	result, err := pc.conn.Query(ctx, `
SELECT 
    n.nspname AS schema_name,
    p.proname AS function_name,
    pg_catalog.pg_get_function_arguments(p.oid) AS function_arguments,
    r.rolname AS owner
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
JOIN pg_roles r ON p.proowner = r.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') -- Exclude system schemas
ORDER BY schema_name, function_name;`)
	if err != nil {
		return functions, err
	}
	defer result.Close()
	for result.Next() {
		var scheme string
		var name string
		var Owner string
		var fArgs string
		err := result.Scan(&scheme, &name, &fArgs, &Owner)
		if err != nil {
			pc.log.Error().Err(err)
		}
		functions = append(functions, Function{Name: name, Scheme: scheme, Owner: Owner, Arguments: fArgs})
	}
	if result.Err() != nil {
		return functions, err
	}
	return functions, nil
}

func (pc *PgConnection) GetIndexes(ctx context.Context) (int, error) {
	pc.log.Info().Msg("getting indexes count")
	var count int
	err := pc.conn.QueryRow(ctx, "SELECT COUNT(*) FROM pg_indexes;").Scan(&count)
	if err != nil {
		pc.log.Error().Err(err)
		return 0, err
	}
	return count, nil
}
func (pc *PgConnection) GetIndexesList(ctx context.Context) ([]Index, error) {
	pc.log.Info().Msg("getting indexes list")
	var indexes []Index
	result, err := pc.conn.Query(ctx, "SELECT tablename, indexname FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog', 'information_schema');")
	if err != nil {
		return indexes, err
	}
	defer result.Close()
	for result.Next() {
		var table string
		var index string
		err := result.Scan(&table, &index)
		if err != nil {
			pc.log.Error().Err(err)
		}
		indexes = append(indexes, Index{Table: table, Index: index})
	}
	if result.Err() != nil {
		return indexes, err
	}
	return indexes, nil
}
func (pc *PgConnection) GetCurrentConnections(ctx context.Context) ([]Connection, error) {
	pc.log.Info().Msg("getting connections list")
	var connections []Connection
	result, err := pc.conn.Query(ctx, "SELECT pid, usename, datname, client_addr, state, query FROM pg_stat_activity;")
	if err != nil {
		return connections, err
	}
	defer result.Close()
	for result.Next() {
		var pid string
		var username string
		var dbname string
		var client_addr string
		var state string
		var query string
		err := result.Scan(&pid, &username, &dbname, &client_addr, &state, &query)
		if err != nil {
			pc.log.Error().Err(err)
		}
		connections = append(connections, Connection{Pid: pid, Username: username, DBname: dbname, ClientAdders: client_addr, Status: state, Query: query})
	}
	if result.Err() != nil {
		return connections, err
	}
	return connections, nil
}

func (pc *PgConnection) GetTableRowCount(ctx context.Context, table Table) (int, error) {
	pc.log.Debug().Msg("getting row count for table")
	var count int
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s."%s"`, table.Scheme, table.Name)
	_, err := pc.conn.Exec(ctx, "SET statement_timeout TO '10min'")
	if err != nil {
		pc.log.Error().Err(err)
		return 0, err
	}
	err = pc.conn.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		pc.log.Error().Err(err)
		return 0, err
	}
	return count, nil
}

func (pc *PgConnection) GetCount(ctx context.Context, query string, queryType string) (int, error) {
	pc.log.Debug().Msgf("getting row count for %s", queryType)
	var count int
	err := pc.conn.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		pc.log.Error().Err(err)
		return 0, err
	}
	return count, nil
}
func (pc *PgConnection) GetString(ctx context.Context, query string, queryType string) (string, error) {
	pc.log.Debug().Msgf("getting string value for %s", queryType)
	var str string
	err := pc.conn.QueryRow(ctx, query).Scan(&str)
	if err != nil {
		pc.log.Error().Err(err)
		return "", err
	}
	return str, nil
}
func (pc *PgConnection) TruncateTables(list []Table, ctx context.Context) error {
	for _, table := range list {
		query := fmt.Sprintf(`TRUNCATE TABLE %s."%s" CASCADE;`, table.Scheme, table.Name)
		pc.log.Debug().Msgf("truncating table %s.%s", table.Scheme, table.Name)
		_, err := pc.conn.Exec(ctx, query)
		if err != nil {
			pc.log.Error().Err(err)
			return err
		}
		pc.log.Info().Msgf("table %s.%s truncated", table.Scheme, table.Name)
	}
	return nil
}
func (pc *PgConnection) CreateDiffSequences(list []Sequence, ctx context.Context) error {
	for _, sequence := range list {
		query := fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS "%s" START 1 INCREMENT 1 OWNED BY tasks.unique_id;`, sequence.Name)
		pc.log.Debug().Msgf("query: %s", query)
		pc.log.Debug().Msgf("creating sequence %s", sequence.Name)
		_, err := pc.conn.Exec(ctx, query)
		if err != nil {
			pc.log.Error().Err(err)
			return err
		}
		pc.log.Info().Msgf("seqeunce %s created", sequence.Name)
	}
	return nil
}
func (pc *PgConnection) ExecuteTransaction(ctx context.Context, query string) error {
	tx, err := pc.conn.Begin(ctx)
	if err != nil {
		pc.log.Error().Err(err)
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, query)
	if err != nil {
		pc.log.Error().Err(err)
		return err
	}
	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}
