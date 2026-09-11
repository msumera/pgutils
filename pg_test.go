package pg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestConnect(t *testing.T) {
	postgres := createContainer(t)
	defer func() {
		_ = postgres.Terminate(context.Background())
	}()

	t.Setenv(EnvMigrationsDirectory, "testdb")
	t.Setenv(EnvChangelogSchema, "testschema")
	pool, err := Connect()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer pool.Close()

	rows, err := pool.Query(context.Background(), "SELECT id, name, description FROM testtable")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	var data []map[string]interface{}
	for rows.Next() {
		var id int
		var name string
		var description string
		err = rows.Scan(&id, &name, &description)
		if err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		data = append(data, map[string]interface{}{
			"id":          id,
			"name":        name,
			"description": description,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	if len(data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(data))
	}
	if data[0]["id"] != 1 {
		t.Errorf("id should be 1, got %v", data[0]["id"])
	}
	if data[0]["name"] != "name1" {
		t.Errorf("name should be name1, got %v", data[0]["name"])
	}
	if data[0]["description"] != "name1" {
		t.Errorf("description should be name1, got %v", data[0]["description"])
	}
}

func TestConnectOtherSchema(t *testing.T) {
	postgres := createContainer(t)
	defer func() {
		_ = postgres.Terminate(context.Background())
	}()

	t.Setenv(EnvMigrationsDirectory, "testdb_schema")
	t.Setenv(EnvChangelogSchema, "testschema")
	t.Setenv(EnvDatabaseSchema, "dbschema")
	pool, err := Connect()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer pool.Close()

	rows, err := pool.Query(context.Background(), "SELECT id, name, description FROM testtable")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	var data []map[string]interface{}
	for rows.Next() {
		var id int
		var name string
		var description string
		err = rows.Scan(&id, &name, &description)
		if err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		data = append(data, map[string]interface{}{
			"id":          id,
			"name":        name,
			"description": description,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	if len(data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(data))
	}
	if data[0]["id"] != 1 {
		t.Errorf("id should be 1, got %v", data[0]["id"])
	}
	if data[0]["name"] != "name1" {
		t.Errorf("name should be name1, got %v", data[0]["name"])
	}
	if data[0]["description"] != "name1" {
		t.Errorf("description should be name1, got %v", data[0]["description"])
	}
}

func TestConnectSchemaInMigration(t *testing.T) {
	postgres := createContainer(t)
	defer func() {
		_ = postgres.Terminate(context.Background())
	}()

	t.Setenv(EnvMigrationsDirectory, "testdb")
	t.Setenv(EnvChangelogSchema, "testschema")
	t.Setenv(EnvDatabaseSchema, "dbschema")
	t.Setenv(EnvDatabaseMigrationSchema, "dbschema")
	pool, err := Connect()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer pool.Close()

	rows, err := pool.Query(context.Background(), "SELECT id, name, description FROM testtable")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	var data []map[string]interface{}
	for rows.Next() {
		var id int
		var name string
		var description string
		err = rows.Scan(&id, &name, &description)
		if err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		data = append(data, map[string]interface{}{
			"id":          id,
			"name":        name,
			"description": description,
		})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration error: %v", err)
	}

	if len(data) != 1 {
		t.Fatalf("expected 1 row, got %d", len(data))
	}
	if data[0]["id"] != 1 {
		t.Errorf("id should be 1, got %v", data[0]["id"])
	}
	if data[0]["name"] != "name1" {
		t.Errorf("name should be name1, got %v", data[0]["name"])
	}
	if data[0]["description"] != "name1" {
		t.Errorf("description should be name1, got %v", data[0]["description"])
	}
}

func TestDoInTransaction(t *testing.T) {
	postgres := createContainer(t)
	defer func() {
		_ = postgres.Terminate(context.Background())
	}()

	t.Setenv(EnvMigrationsDirectory, "testdb")
	pool, err := Connect()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// 1. Success case
	insertedID, err := DoInTransaction(ctx, pool, func(tx pgx.Tx) (int, error) {
		var id int
		err := tx.QueryRow(ctx, "INSERT INTO testtable (id, name, description) VALUES (2, 'tx_test', 'desc') RETURNING id").Scan(&id)
		return id, err
	})
	if err != nil {
		t.Fatalf("DoInTransaction failed: %v", err)
	}
	if insertedID != 2 {
		t.Errorf("expected ID 2, got %d", insertedID)
	}

	// Verify insertion persisted
	var count int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM testtable WHERE name = 'tx_test'").Scan(&count); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}

	// 2. Failure rollback case
	expectedErr := errors.New("deliberate rollback error")
	_, err = DoInTransaction(ctx, pool, func(tx pgx.Tx) (int, error) {
		_, execErr := tx.Exec(ctx, "INSERT INTO testtable (id, name, description) VALUES (3, 'rollback_test', 'desc')")
		if execErr != nil {
			return 0, execErr
		}
		return 0, expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected deliberate error, got: %v", err)
	}

	// Verify rollback took effect
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM testtable WHERE name = 'rollback_test'").Scan(&count); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows after rollback, got %d", count)
	}

	// 3. DoInTransactionNoResult success & rollback
	err = DoInTransactionNoResult(ctx, pool, func(tx pgx.Tx) error {
		_, execErr := tx.Exec(ctx, "INSERT INTO testtable (id, name, description) VALUES (4, 'no_result', 'desc')")
		return execErr
	})
	if err != nil {
		t.Fatalf("DoInTransactionNoResult failed: %v", err)
	}

	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM testtable WHERE name = 'no_result'").Scan(&count); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

func TestCreateConfigurationFromEnv(t *testing.T) {
	// Test defaults
	cfg := CreateConfigurationFromEnv()
	if cfg.Address != EnvDatabaseAddressDefault {
		t.Errorf("expected address %s, got %s", EnvDatabaseAddressDefault, cfg.Address)
	}
	if cfg.Username != EnvDatabaseUsernameDefault {
		t.Errorf("expected username %s, got %s", EnvDatabaseUsernameDefault, cfg.Username)
	}
	if cfg.MigrationsEnabled != true {
		t.Errorf("expected migrations enabled true, got %v", cfg.MigrationsEnabled)
	}

	// Test overrides
	t.Setenv(EnvDatabaseAddress, "customhost:5433")
	t.Setenv(EnvDatabaseUsername, "custom_user")
	t.Setenv(EnvMigrationsEnabled, "false")
	t.Setenv(EnvDatabaseSslMode, "require")

	cfg2 := CreateConfigurationFromEnv()
	if cfg2.Address != "customhost:5433" {
		t.Errorf("expected address customhost:5433, got %s", cfg2.Address)
	}
	if cfg2.Username != "custom_user" {
		t.Errorf("expected username custom_user, got %s", cfg2.Username)
	}
	if cfg2.MigrationsEnabled != false {
		t.Errorf("expected migrations enabled false, got %v", cfg2.MigrationsEnabled)
	}
	if cfg2.SslMode != SSLModeRequire {
		t.Errorf("expected sslmode require, got %s", cfg2.SslMode)
	}
}

func TestMigrationSortingAndFiltering(t *testing.T) {
	tempDir := t.TempDir()

	files := []string{
		"1_add_users.sql",
		"0_1_seed_roles.sql",
		"0_init_schema.sql",
		"1_add_profiles.sql", // Same numeric ID (1) as 1_add_users.sql -> tie-breaker by filename
		"2_3_4_complex_version.sql",
		"ignored_non_numeric.sql",
		"not_sql.txt",
	}

	for _, f := range files {
		if err := os.WriteFile(filepath.Join(tempDir, f), []byte("SELECT 1;"), 0644); err != nil {
			t.Fatalf("failed to create test file %s: %v", f, err)
		}
	}

	migrator := &databaseMigrator{
		Configuration: Configuration{
			MigrationsDirectory: tempDir,
		},
	}

	migrations, err := migrator.getMigrations()
	if err != nil {
		t.Fatalf("getMigrations failed: %v", err)
	}

	expectedFiles := []string{
		"0_init_schema.sql",
		"0_1_seed_roles.sql",
		"1_add_profiles.sql", // 'add_profiles' < 'add_users' alphabetically
		"1_add_users.sql",
		"2_3_4_complex_version.sql",
	}

	if len(migrations) != len(expectedFiles) {
		t.Fatalf("expected %d migrations, got %d", len(expectedFiles), len(migrations))
	}

	for i, expected := range expectedFiles {
		if migrations[i].Filename != expected {
			t.Errorf("at index %d: expected %s, got %s", i, expected, migrations[i].Filename)
		}
	}
}

func createContainer(t *testing.T) testcontainers.Container {
	t.Helper()
	containerRequest := testcontainers.ContainerRequest{
		Image:        "postgres:17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test_user",
			"POSTGRES_PASSWORD": "test_password",
			"POSTGRES_DB":       "test_db",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithPollInterval(time.Second),
	}
	postgres, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: containerRequest,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to create postgres container: %v", err)
	}
	configure(t, postgres)
	return postgres
}

func configure(t *testing.T, postgres testcontainers.Container) {
	t.Helper()
	port, err := postgres.MappedPort(context.Background(), "5432")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}
	t.Setenv(EnvDatabaseAddress, "localhost:"+port.Port())
	t.Setenv(EnvDatabaseUsername, "test_user")
	t.Setenv(EnvDatabasePassword, "test_password")
	t.Setenv(EnvDatabaseName, "test_db")
	t.Setenv(EnvMigrationsEnabled, "true")
}
