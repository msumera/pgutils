package pg

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MigrationStatus represents the status of a database migration.
type MigrationStatus string

// SSLMode represents PostgreSQL SSL connection mode.
type SSLMode string
type SslMode = SSLMode

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
	EnvDatabaseSslModeDefault = SSLModeDisable

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

	StatusCompleted MigrationStatus = "COMPLETED"
	StatusError     MigrationStatus = "ERROR"
	StatusNew       MigrationStatus = "NEW"

	statusCompleted = StatusCompleted
	statusError     = StatusError
	statusNew       = StatusNew

	SSLModeDisable    SSLMode = "disable"
	SSLModeRequire    SSLMode = "require"
	SSLModeVerifyFull SSLMode = "verify-full"
	SSLModeVerifyCA   SSLMode = "verify-ca"
	SSLModePrefer     SSLMode = "prefer"
	SSLModeAllow      SSLMode = "allow"

	SslModeDisable    = SSLModeDisable
	SslModeRequire    = SSLModeRequire
	SslModeVerifyFull = SSLModeVerifyFull
	SslModeVerifyCA   = SSLModeVerifyCA
	SslModePrefer     = SSLModePrefer
	SslModeAllow      = SSLModeAllow
)

// Configuration holds the PostgreSQL connection and migration settings.
type Configuration struct {
	Address         string
	Username        string
	Password        string
	Name            string
	Schema          string
	MigrationSchema string
	SslMode         SSLMode
	SslRootCert     string
	SslCert         string
	SslKey          string

	MigrationsEnabled   bool
	ChangelogSchema     string
	ChangelogTable      string
	MigrationsDirectory string
}

// CreateConfigurationFromEnv creates a Configuration by reading environment variables
// or falling back to default values.
func CreateConfigurationFromEnv() Configuration {
	getEnv := func(key, def string) string {
		if val, ok := os.LookupEnv(key); ok && val != "" {
			return val
		}
		return def
	}

	migrationsEnabled := EnvMigrationsEnabledDefault
	if val, ok := os.LookupEnv(EnvMigrationsEnabled); ok {
		if parsed, err := strconv.ParseBool(val); err == nil {
			migrationsEnabled = parsed
		}
	}

	return Configuration{
		Address:             getEnv(EnvDatabaseAddress, EnvDatabaseAddressDefault),
		Username:            getEnv(EnvDatabaseUsername, EnvDatabaseUsernameDefault),
		Password:            getEnv(EnvDatabasePassword, EnvDatabasePasswordDefault),
		Name:                getEnv(EnvDatabaseName, EnvDatabaseNameDefault),
		Schema:              getEnv(EnvDatabaseSchema, EnvDatabaseSchemaDefault),
		MigrationSchema:     getEnv(EnvDatabaseMigrationSchema, EnvDatabaseMigrationSchemaDefault),
		SslMode:             SSLMode(getEnv(EnvDatabaseSslMode, string(EnvDatabaseSslModeDefault))),
		SslRootCert:         getEnv(EnvDatabaseSslRootCert, EnvDatabaseSslRootCertDefault),
		SslCert:             getEnv(EnvDatabaseSslCert, EnvDatabaseSslCertDefault),
		SslKey:              getEnv(EnvDatabaseSslKey, EnvDatabaseSslKeyDefault),
		MigrationsEnabled:   migrationsEnabled,
		ChangelogSchema:     getEnv(EnvChangelogSchema, EnvChangelogSchemaDefault),
		ChangelogTable:      getEnv(EnvChangelogTable, EnvChangelogTableDefault),
		MigrationsDirectory: getEnv(EnvMigrationsDirectory, EnvMigrationsDirectoryDefault),
	}
}

func (c Configuration) schemaTable() string {
	if c.ChangelogSchema == "" {
		return pgx.Identifier{c.ChangelogTable}.Sanitize()
	}
	return pgx.Identifier{c.ChangelogSchema, c.ChangelogTable}.Sanitize()
}

// Connect creates a connection pool from environment variables.
func Connect() (*pgxpool.Pool, error) {
	return ConnectContext(context.Background())
}

// ConnectContext creates a connection pool from environment variables with the given context.
func ConnectContext(ctx context.Context) (*pgxpool.Pool, error) {
	return ConnectWithConfigContext(ctx, CreateConfigurationFromEnv())
}

// ConnectWithConfig creates a connection pool using the provided Configuration.
func ConnectWithConfig(c Configuration) (*pgxpool.Pool, error) {
	return ConnectWithConfigContext(context.Background(), c)
}

// ConnectWithConfigContext creates a connection pool using the provided Configuration and context.
func ConnectWithConfigContext(ctx context.Context, c Configuration) (*pgxpool.Pool, error) {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   c.Address,
		Path:   c.Name,
	}

	q := url.Values{}
	if c.SslMode != "" {
		q.Set("sslmode", string(c.SslMode))
	}
	if c.SslRootCert != "" {
		q.Set("sslrootcert", c.SslRootCert)
	}
	if c.SslCert != "" {
		q.Set("sslcert", c.SslCert)
	}
	if c.SslKey != "" {
		q.Set("sslkey", c.SslKey)
	}
	if len(q) > 0 {
		u.RawQuery = q.Encode()
	}

	config, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %w", err)
	}
	if c.Schema != "" {
		config.ConnConfig.RuntimeParams["search_path"] = c.Schema
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if c.MigrationsEnabled {
		dm := createDatabaseMigrator(pool, c)
		if err := dm.Migrate(ctx); err != nil {
			pool.Close()
			return nil, fmt.Errorf("migration failed: %w", err)
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
	ID       []int
	Name     string
	Filename string
}

func (dbm *databaseMigrator) Migrate(ctx context.Context) error {
	if err := dbm.initChangelogTable(ctx); err != nil {
		return fmt.Errorf("failed to init changelog table: %w", err)
	}
	migrations, err := dbm.getMigrations()
	if err != nil {
		return fmt.Errorf("failed to retrieve migrations: %w", err)
	}
	if len(migrations) == 0 {
		return nil
	}

	tx, err := dbm.PgxPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin migration transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	lockQuery := fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", dbm.Configuration.schemaTable())
	if _, err := tx.Exec(ctx, lockQuery); err != nil {
		return fmt.Errorf("failed to lock changelog table: %w", err)
	}

	if dbm.Configuration.MigrationSchema != "" {
		exists, err := dbm.schemaExists(ctx, dbm.Configuration.MigrationSchema)
		if err != nil {
			return fmt.Errorf("failed to check migration schema: %w", err)
		}
		if !exists {
			if err := dbm.createSchema(ctx, dbm.Configuration.MigrationSchema); err != nil {
				return fmt.Errorf("failed to create migration schema: %w", err)
			}
		}
		schemaIdent := pgx.Identifier{dbm.Configuration.MigrationSchema}.Sanitize()
		if _, err := tx.Exec(ctx, "SET search_path TO "+schemaIdent); err != nil {
			return fmt.Errorf("failed to set search_path to migration schema: %w", err)
		}
	}

	for _, m := range migrations {
		if err := dbm.applyMigration(ctx, m, tx); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migrations: %w", err)
	}
	return nil
}

func formatMigrationID(id []int) string {
	parts := make([]string, len(id))
	for i, v := range id {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, ".")
}

func (dbm *databaseMigrator) applyMigration(ctx context.Context, m migration, tx pgx.Tx) error {
	slog.Info("Applying migration", "filename", m.Filename)
	id := formatMigrationID(m.ID)
	status, err := dbm.getMigrationStatus(ctx, id, tx)
	if err != nil {
		return fmt.Errorf("failed to get migration status for %s: %w", m.Filename, err)
	}
	if status == StatusCompleted {
		slog.Info("Migration already applied", "filename", m.Filename)
		return nil
	}

	filePath := filepath.Join(dbm.Configuration.MigrationsDirectory, m.Filename)
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		slog.Error("Error reading migration file", "filename", m.Filename, "error", err)
		return fmt.Errorf("failed to read migration file %s: %w", m.Filename, err)
	}

	script := string(bytes)
	if _, err := tx.Exec(ctx, script); err != nil {
		slog.Error("Failed to apply migration script", "filename", m.Filename, "error", err)
		return fmt.Errorf("failed to execute migration %s: %w", m.Filename, err)
	}

	slog.Info("Migration status", "filename", m.Filename, "status", StatusCompleted)
	if err := dbm.updateMigrationStatus(ctx, id, m, StatusCompleted, tx); err != nil {
		return fmt.Errorf("failed to update migration status for %s: %w", m.Filename, err)
	}
	return nil
}

func (dbm *databaseMigrator) getMigrationStatus(ctx context.Context, id string, tx pgx.Tx) (MigrationStatus, error) {
	query := fmt.Sprintf("SELECT status FROM %s WHERE id = $1 FOR UPDATE", dbm.Configuration.schemaTable())
	row := tx.QueryRow(ctx, query, id)
	var status MigrationStatus
	err := row.Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return StatusNew, nil
	}
	if err != nil {
		return "", err
	}
	return status, nil
}

func (dbm *databaseMigrator) updateMigrationStatus(ctx context.Context, id string, m migration, status MigrationStatus, tx pgx.Tx) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (id, name, filename, status, timestamp) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET status = $4, timestamp = $5",
		dbm.Configuration.schemaTable(),
	)
	_, err := tx.Exec(ctx, query, id, m.Name, m.Filename, string(status), time.Now())
	if err != nil {
		slog.Error("Error inserting migration info", "filename", m.Filename, "error", err)
		return err
	}
	return nil
}

func (dbm *databaseMigrator) getMigrations() ([]migration, error) {
	migrationsDir := dbm.Configuration.MigrationsDirectory
	entries, err := os.ReadDir(migrationsDir)
	if errors.Is(err, fs.ErrNotExist) {
		slog.Warn("Directory does not exist", "directory", dbm.Configuration.MigrationsDirectory)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory %s: %w", migrationsDir, err)
	}

	var migrations []migration
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		parts := strings.Split(entry.Name(), "_")
		var ids []int
		for _, part := range parts {
			v, err := strconv.Atoi(part)
			if err != nil {
				break
			}
			ids = append(ids, v)
		}

		// Files without numeric version prefix are ignored per documentation
		if len(ids) == 0 {
			slog.Warn("Ignoring migration file without numeric prefix", "filename", entry.Name())
			continue
		}

		names := parts[len(ids):]
		name := strings.TrimSuffix(strings.Join(names, " "), ".sql")
		migrations = append(migrations, migration{
			ID:       ids,
			Name:     name,
			Filename: entry.Name(),
		})
	}

	slices.SortFunc(migrations, func(m1, m2 migration) int {
		minLen := min(len(m1.ID), len(m2.ID))
		for i := 0; i < minLen; i++ {
			if m1.ID[i] < m2.ID[i] {
				return -1
			}
			if m1.ID[i] > m2.ID[i] {
				return 1
			}
		}
		if len(m1.ID) < len(m2.ID) {
			return -1
		}
		if len(m1.ID) > len(m2.ID) {
			return 1
		}
		// Tie-breaker: compare filenames alphabetically
		return strings.Compare(m1.Filename, m2.Filename)
	})

	return migrations, nil
}

func (dbm *databaseMigrator) initChangelogTable(ctx context.Context) error {
	exists, err := dbm.tableExists(ctx, dbm.Configuration.ChangelogSchema, dbm.Configuration.ChangelogTable)
	if err != nil {
		return err
	}
	if !exists {
		if err := dbm.createChangelogTable(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (dbm *databaseMigrator) schemaExists(ctx context.Context, schema string) (bool, error) {
	querySQL := "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)"
	row := dbm.PgxPool.QueryRow(ctx, querySQL, schema)
	var exists bool
	if err := row.Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check if schema exists: %w", err)
	}
	return exists, nil
}

func (dbm *databaseMigrator) tableExists(ctx context.Context, schema string, table string) (bool, error) {
	var querySQL string
	var row pgx.Row
	if schema != "" {
		querySQL = "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = $1 AND tablename = $2)"
		row = dbm.PgxPool.QueryRow(ctx, querySQL, schema, table)
	} else {
		querySQL = "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $1)"
		row = dbm.PgxPool.QueryRow(ctx, querySQL, table)
	}
	var exists bool
	if err := row.Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check if table exists: %w", err)
	}
	return exists, nil
}

func (dbm *databaseMigrator) createSchema(ctx context.Context, schema string) error {
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := dbm.PgxPool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", ident)); err != nil {
		return fmt.Errorf("failed to create schema %s: %w", schema, err)
	}
	return nil
}

func (dbm *databaseMigrator) createChangelogTable(ctx context.Context) error {
	tx, err := dbm.PgxPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if dbm.Configuration.ChangelogSchema != "" {
		schemaIdent := pgx.Identifier{dbm.Configuration.ChangelogSchema}.Sanitize()
		if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaIdent)); err != nil {
			return fmt.Errorf("failed to create changelog schema: %w", err)
		}
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id TEXT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL,
		filename TEXT NOT NULL,
		status TEXT NOT NULL,
		timestamp TIMESTAMPTZ NOT NULL
	)`, dbm.Configuration.schemaTable())

	if _, err := tx.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create changelog table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit changelog table creation: %w", err)
	}
	return nil
}

// DoInTransaction executes fn within a database transaction with context support.
// If fn returns an error, the transaction is rolled back; otherwise it is committed.
func DoInTransaction[R any](ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) (R, error)) (R, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		var zero R
		return zero, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	result, err := fn(tx)
	if err != nil {
		var zero R
		return zero, err
	}

	if err := tx.Commit(ctx); err != nil {
		var zero R
		return zero, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return result, nil
}

// DoInTransactionNoResult executes fn within a database transaction without returning a result.
// If fn returns an error, the transaction is rolled back; otherwise it is committed.
func DoInTransactionNoResult(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}
