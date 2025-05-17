package pg

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"io"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type migrationStatus = string
type SslMode = string

const (
	EnvDatabaseAddress        = "DB_ADDRESS"
	EnvDatabaseAddressDefault = "localhost:5432"

	EnvDatabaseUsername        = "DB_USERNAME"
	EnvDatabaseUsernameDefault = "postgres"

	EnvDatabasePassword        = "DB_PASSWORD"
	EnvDatabasePasswordDefault = "postgres"

	EnvDatabaseName        = "DB_NAME"
	EnvDatabaseNameDefault = "postgres"

	EnvDatabaseSchema        = "DB_SCHEMA"
	EnvDatabaseSchemaDefault = ""

	EnvDatabaseMigrationSchema        = "DB_MIGRATION_SCHEMA"
	EnvDatabaseMigrationSchemaDefault = ""

	EnvDatabaseSslMode        = "DB_SSL_MODE"
	EnvDatabaseSslModeDefault = SslModeDisable

	EnvDatabaseSslRootCert        = "DB_SSL_ROOT_CERT"
	EnvDatabaseSslRootCertDefault = ""

	EnvDatabaseSslCert        = "DB_SSL_CERT"
	EnvDatabaseSslCertDefault = ""

	EnvDatabaseSslKey        = "DB_SSL_KEY"
	EnvDatabaseSslKeyDefault = ""

	EnvMigrationsEnabled        = "DB_MIGRATIONS_ENABLED"
	EnvMigrationsEnabledDefault = true

	EnvChangelogSchema        = "DB_CHANGELOG_SCHEMA"
	EnvChangelogSchemaDefault = "public"

	EnvChangelogTable        = "DB_CHANGELOG_TABLE"
	EnvChangelogTableDefault = "changelog"

	EnvMigrationsDirectory        = "DB_MIGRATIONS_DIRECTORY"
	EnvMigrationsDirectoryDefault = "db"

	statusCompleted migrationStatus = "COMPLETED"
	statusError     migrationStatus = "ERROR"
	statusNew       migrationStatus = "NEW"

	SslModeDisable    SslMode = "disable"
	SslModeRequire    SslMode = "require"
	SslModeVerifyFull SslMode = "verify-full"
	SslModeVerifyCA   SslMode = "verify-ca"
	SslModePrefer     SslMode = "prefer"
	SslModeAllow      SslMode = "allow"
)

type Configuration struct {
	Address         string
	Username        string
	Password        string
	Name            string
	Schema          string
	MigrationSchema string
	SslMode         SslMode
	SslRootCert     string
	SslCert         string
	SslKey          string

	MigrationsEnabled   bool
	ChangelogSchema     string
	ChangelogTable      string
	MigrationsDirectory string
}

func CreateConfigurationFromEnv() Configuration {
	address := os.Getenv(EnvDatabaseAddress)
	if address == "" {
		address = EnvDatabaseAddressDefault
	}
	username := os.Getenv(EnvDatabaseUsername)
	if username == "" {
		username = EnvDatabaseUsernameDefault
	}
	password := os.Getenv(EnvDatabasePassword)
	if password == "" {
		password = EnvDatabasePasswordDefault
	}
	name := os.Getenv(EnvDatabaseName)
	if name == "" {
		name = EnvDatabaseNameDefault
	}
	schema := os.Getenv(EnvDatabaseSchema)
	if schema == "" {
		schema = EnvDatabaseSchemaDefault
	}
	migrationSchema := os.Getenv(EnvDatabaseMigrationSchema)
	if migrationSchema == "" {
		migrationSchema = EnvDatabaseMigrationSchemaDefault
	}
	sslMode := os.Getenv(EnvDatabaseSslMode)
	if sslMode == "" {
		sslMode = EnvDatabaseSslModeDefault
	}
	sslRootCert := os.Getenv(EnvDatabaseSslRootCert)
	if sslRootCert == "" {
		sslRootCert = EnvDatabaseSslRootCertDefault
	}
	sslCert := os.Getenv(EnvDatabaseSslCert)
	if sslCert == "" {
		sslCert = EnvDatabaseSslCertDefault
	}
	sslKey := os.Getenv(EnvDatabaseSslKey)
	if sslKey == "" {
		sslKey = EnvDatabaseSslKeyDefault
	}

	migrationsEnabled, err := strconv.ParseBool(os.Getenv(EnvMigrationsEnabled))
	if err != nil {
		migrationsEnabled = EnvMigrationsEnabledDefault
	}

	changelogSchema := os.Getenv(EnvChangelogSchema)
	if changelogSchema == "" {
		changelogSchema = EnvChangelogSchemaDefault
	}
	changelogTable := os.Getenv(EnvChangelogTable)
	if changelogTable == "" {
		changelogTable = EnvChangelogTableDefault
	}
	migrationsDirectory := os.Getenv(EnvMigrationsDirectory)
	if migrationsDirectory == "" {
		migrationsDirectory = EnvMigrationsDirectoryDefault
	}
	return Configuration{
		Address:             address,
		Username:            username,
		Password:            password,
		Name:                name,
		Schema:              schema,
		MigrationSchema:     migrationSchema,
		SslMode:             sslMode,
		SslRootCert:         sslRootCert,
		SslCert:             sslCert,
		SslKey:              sslKey,
		MigrationsEnabled:   migrationsEnabled,
		ChangelogSchema:     changelogSchema,
		ChangelogTable:      changelogTable,
		MigrationsDirectory: migrationsDirectory,
	}
}

func (c Configuration) schemaTable() string {
	if c.ChangelogSchema == "" {
		return c.ChangelogTable
	}
	return c.ChangelogSchema + "." + c.ChangelogTable
}

func Connect() (*pgxpool.Pool, error) {
	c := CreateConfigurationFromEnv()
	return ConnectWithConfig(c)
}

func ConnectWithConfig(c Configuration) (*pgxpool.Pool, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s", c.Username, c.Password, c.Address, c.Name, c.SslMode)
	if c.SslRootCert != "" {
		url += "&sslrootcert=" + c.SslRootCert
	}
	if c.SslCert != "" {
		url += "&sslcert=" + c.SslCert
	}
	if c.SslKey != "" {
		url += "&sslkey=" + c.SslKey
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if c.Schema != "" {
		config.ConnConfig.RuntimeParams["search_path"] = c.Schema
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}
	if c.MigrationsEnabled {
		dm := createDatabaseMigrator(pool, c)
		err = dm.Migrate()
		if err != nil {
			return nil, err
		}
	}
	return pool, nil
}

type databaseMigrator struct {
	PgxPool       *pgxpool.Pool
	Configuration Configuration
}

func createDatabaseMigrator(pgxPool *pgxpool.Pool, config Configuration) *databaseMigrator {
	return &databaseMigrator{
		PgxPool:       pgxPool,
		Configuration: config,
	}
}

type migration struct {
	Id       []int
	Name     string
	Filename string
}

func (dbm *databaseMigrator) Migrate() error {
	err := dbm.initChangelogTable()
	if err != nil {
		return err
	}
	migrations, err := dbm.getMigrations()
	if err != nil {
		return err
	}
	tx, err := dbm.PgxPool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(context.Background())
			panic(p)
		}
	}()
	_, err = tx.Exec(context.Background(), dbm.replaceEnv("LOCK TABLE {SCHEMA_TABLE} IN ACCESS EXCLUSIVE MODE"))
	if err != nil {
		return err
	}
	if dbm.Configuration.MigrationSchema != "" {
		exists, err := dbm.schemaExists(dbm.Configuration.MigrationSchema)
		if err != nil {
			return err
		}
		if !exists {
			err = dbm.createSchema(dbm.Configuration.MigrationSchema)
			if err != nil {
				return err
			}
		}
		_, err = tx.Exec(context.Background(), "SET search_path TO "+dbm.Configuration.MigrationSchema)
		if err != nil {
			return err
		}
	}
	for _, migration := range migrations {
		err = dbm.applyMigration(migration, tx)
		if err != nil {
			return err
		}
	}
	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func Map[T, R any](list []T, fn func(T) R) []R {
	result := make([]R, 0, len(list))
	for _, t := range list {
		result = append(result, fn(t))
	}
	return result
}

func (dbm *databaseMigrator) applyMigration(migration migration, tx pgx.Tx) error {
	log.Printf("Applying migration %v", migration.Filename)
	id := strings.Join(Map(migration.Id, strconv.Itoa), ".")
	status, err := dbm.getMigrationStatus(id, tx)
	if err != nil {
		return err
	}
	if status == statusCompleted {
		log.Printf("Migration %v already applied", migration.Filename)
		return nil
	}
	scriptFile, err := os.Open(dbm.Configuration.MigrationsDirectory + string(os.PathSeparator) + migration.Filename)
	if err != nil {
		log.Printf("Error opening migration file %v: %v", migration.Filename, err)
		return err
	}
	defer func(scriptFile *os.File) {
		_ = scriptFile.Close()
	}(scriptFile)
	bytes, err := io.ReadAll(scriptFile)
	if err != nil {
		log.Printf("Error reading migration file %v: %v", migration.Filename, err)
		return err
	}
	script := string(bytes)
	_, migrationError := tx.Exec(context.Background(), script)
	if migrationError != nil {
		status = statusError
	} else {
		status = statusCompleted
	}
	log.Printf("Migration status: %v", status)
	err = dbm.updateMigrationStatus(id, migration, status, tx)
	if err != nil {
		return err
	}
	return migrationError
}

func (dbm *databaseMigrator) getMigrationStatus(id string, tx pgx.Tx) (migrationStatus, error) {
	//goland:noinspection SqlResolve
	query := dbm.replaceEnv("SELECT status FROM {SCHEMA_TABLE} WHERE id = $1 FOR UPDATE")
	row := tx.QueryRow(context.Background(), query, id)
	var migrationStatus migrationStatus
	err := row.Scan(&migrationStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return statusNew, nil
	}
	if err != nil {
		return "", err
	}
	return migrationStatus, nil
}

func (dbm *databaseMigrator) updateMigrationStatus(id string, migration migration, status migrationStatus, tx pgx.Tx) error {
	//goland:noinspection SqlResolve
	insert := dbm.replaceEnv("INSERT INTO {SCHEMA_TABLE} (id, name, filename, status, timestamp) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET status = $4, timestamp = $5")
	_, err := tx.Exec(context.Background(), insert, id, migration.Name, migration.Filename, status, time.Now())
	if err != nil {
		log.Printf("Error inserting migration info %v: %v", migration.Filename, err)
		return err
	}
	return nil
}

func (dbm *databaseMigrator) getMigrations() ([]migration, error) {
	migrationsDir := dbm.Configuration.MigrationsDirectory
	entries, err := os.ReadDir(migrationsDir)
	if errors.Is(err, fs.ErrNotExist) {
		log.Warnf("Directory %v does not exist", dbm.Configuration.MigrationsDirectory)
		return make([]migration, 0), nil
	}
	if err != nil {
		return nil, err
	}
	migrations := make([]migration, 0)
	for i := range entries {
		entry := entries[i]
		if !entry.IsDir() {
			if strings.HasSuffix(entry.Name(), ".sql") {
				parts := strings.Split(entry.Name(), "_")
				ids := make([]int, 0)
				for _, part := range parts {
					v, err := strconv.Atoi(part)
					if err == nil {
						ids = append(ids, v)
					} else {
						break
					}
				}
				names := make([]string, 0)
				for i := 0; i < len(parts)-len(ids); i++ {
					names = append(names, parts[i+len(ids)])
				}
				name := strings.TrimSuffix(strings.Join(names, " "), ".sql")
				migration := migration{
					Id:       ids,
					Name:     name,
					Filename: entry.Name(),
				}
				migrations = append(migrations, migration)
			}
		}
	}
	sort.Slice(migrations, func(i, j int) bool {
		m1 := migrations[i].Id
		m2 := migrations[j].Id
		for i := 0; i < min(len(m1), len(m2)); i++ {
			i1 := m1[i]
			i2 := m2[i]
			if i1 < i2 {
				return true
			}
			if i1 > i2 {
				return false
			}
		}
		if len(m1) < len(m2) {
			return true
		}
		return false
	})
	return migrations, nil
}

func (dbm *databaseMigrator) initChangelogTable() error {
	exists, err := dbm.tableExists(dbm.Configuration.ChangelogSchema, dbm.Configuration.ChangelogTable)
	if err != nil {
		return err
	}
	if !exists {
		err = dbm.createChangelogTable()
		if err != nil {
			return err
		}
	}
	return nil
}

func (dbm *databaseMigrator) schemaExists(schema string) (bool, error) {
	querySql := "SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schemata.schema_name = $1)"
	row := dbm.PgxPool.QueryRow(context.Background(), querySql, schema)
	var exists bool
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (dbm *databaseMigrator) tableExists(schema string, table string) (bool, error) {
	//goland:noinspection SqlResolve
	querySql := "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = $1 AND tablename = $2)"
	row := dbm.PgxPool.QueryRow(context.Background(), querySql, schema, table)
	var exists bool
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (dbm *databaseMigrator) createSchema(schema string) error {
	_, err := dbm.PgxPool.Exec(context.Background(), "CREATE SCHEMA IF NOT EXISTS "+schema)
	if err != nil {
		return err
	}
	return nil
}

func (dbm *databaseMigrator) createChangelogTable() error {
	tx, err := dbm.PgxPool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(context.Background())
			panic(p)
		}
	}()
	script := `
 		CREATE SCHEMA IF NOT EXISTS {SCHEMA};
		CREATE TABLE IF NOT EXISTS {SCHEMA_TABLE}
		(
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			filename TEXT NOT NULL,
			status TEXT NOT NULL,
			timestamp TIMESTAMPTZ NOT NULL
		);
	`
	_, err = tx.Exec(context.Background(), dbm.replaceEnv(script))
	if err != nil {
		return err
	}
	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (dbm *databaseMigrator) replaceEnv(s string) string {
	s = strings.ReplaceAll(s, "{SCHEMA_TABLE}", dbm.Configuration.schemaTable())
	s = strings.ReplaceAll(s, "{SCHEMA}", dbm.Configuration.ChangelogSchema)
	return s
}

func DoInTransaction[R any](pool *pgxpool.Pool, fn func(tx pgx.Tx) (*R, error)) (*R, error) {
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return nil, err
	}
	defer func(tx pgx.Tx, ctx context.Context) {
		_ = tx.Rollback(ctx)
	}(tx, context.Background())
	result, err := fn(tx)
	if err != nil {
		return nil, err
	}
	err = tx.Commit(context.Background())
	if err != nil {
		return nil, err
	}
	return result, nil
}

func DoInTransactionNoResult(pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := pool.Begin(context.Background())
	if err != nil {
		return err
	}
	defer func(tx pgx.Tx, ctx context.Context) {
		_ = tx.Rollback(ctx)
	}(tx, context.Background())
	err = fn(tx)
	if err != nil {
		return err
	}
	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}
	return nil
}
