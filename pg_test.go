package pg

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
	"time"
)

func TestConnect(t *testing.T) {
	postgres := createContainer(t)
	defer func(postgres testcontainers.Container, ctx context.Context) {
		_ = postgres.Terminate(ctx)
	}(postgres, context.Background())

	t.Setenv(EnvMigrationsDirectory, "testdb")
	t.Setenv(EnvChangelogSchema, "testschema")
	pool, err := Connect()
	if err != nil {
		t.Error(err)
	}

	rows, err := pool.Query(context.Background(), "SELECT id, name, description FROM testtable")
	if err != nil {
		t.Error(err)
	}
	data := make([]map[string]interface{}, 0)
	for rows.Next() {
		var id int
		var name string
		var description string
		err = rows.Scan(&id, &name, &description)
		if err != nil {
			t.Error(err)
		}
		data = append(data, map[string]interface{}{
			"id":          id,
			"name":        name,
			"description": description,
		})
	}
	if len(data) != 1 {
		t.Error("data len is not 1")
	}
	if data[0]["id"] != 1 {
		t.Error("id should be 1")
	}
	if data[0]["name"] != "name1" {
		t.Error("name should be name1")
	}
	if data[0]["description"] != "name1" {
		t.Error("description should be name1")
	}
}

func TestConnectOtherSchema(t *testing.T) {
	postgres := createContainer(t)
	defer func(postgres testcontainers.Container, ctx context.Context) {
		_ = postgres.Terminate(ctx)
	}(postgres, context.Background())

	t.Setenv(EnvMigrationsDirectory, "testdb_schema")
	t.Setenv(EnvChangelogSchema, "testschema")
	t.Setenv(EnvDatabaseSchema, "dbschema")
	pool, err := Connect()
	if err != nil {
		t.Error(err)
	}

	rows, err := pool.Query(context.Background(), "SELECT id, name, description FROM testtable")
	if err != nil {
		t.Error(err)
	}
	data := make([]map[string]interface{}, 0)
	for rows.Next() {
		var id int
		var name string
		var description string
		err = rows.Scan(&id, &name, &description)
		if err != nil {
			t.Error(err)
		}
		data = append(data, map[string]interface{}{
			"id":          id,
			"name":        name,
			"description": description,
		})
	}
	if len(data) != 1 {
		t.Error("data len is not 1")
	}
	if data[0]["id"] != 1 {
		t.Error("id should be 1")
	}
	if data[0]["name"] != "name1" {
		t.Error("name should be name1")
	}
	if data[0]["description"] != "name1" {
		t.Error("description should be name1")
	}
}

func TestConnectSchemaInMigration(t *testing.T) {
	postgres := createContainer(t)
	defer func(postgres testcontainers.Container, ctx context.Context) {
		_ = postgres.Terminate(ctx)
	}(postgres, context.Background())

	t.Setenv(EnvMigrationsDirectory, "testdb")
	t.Setenv(EnvChangelogSchema, "testschema")
	t.Setenv(EnvDatabaseSchema, "dbschema")
	t.Setenv(EnvDatabaseMigrationSchema, "dbschema")
	pool, err := Connect()
	if err != nil {
		t.Error(err)
	}

	rows, err := pool.Query(context.Background(), "SELECT id, name, description FROM testtable")
	if err != nil {
		t.Error(err)
	}
	data := make([]map[string]interface{}, 0)
	for rows.Next() {
		var id int
		var name string
		var description string
		err = rows.Scan(&id, &name, &description)
		if err != nil {
			t.Error(err)
		}
		data = append(data, map[string]interface{}{
			"id":          id,
			"name":        name,
			"description": description,
		})
	}
	if len(data) != 1 {
		t.Error("data len is not 1")
	}
	if data[0]["id"] != 1 {
		t.Error("id should be 1")
	}
	if data[0]["name"] != "name1" {
		t.Error("name should be name1")
	}
	if data[0]["description"] != "name1" {
		t.Error("description should be name1")
	}
}

func createContainer(t *testing.T) testcontainers.Container {
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
		t.Error(err)
	}
	configure(t, postgres)
	return postgres
}

func configure(t *testing.T, postgres testcontainers.Container) {
	port, err := postgres.MappedPort(context.Background(), "5432")
	if err != nil {
		t.Error(err)
	}
	t.Setenv(EnvDatabaseAddress, "localhost:"+port.Port())
	t.Setenv(EnvDatabaseUsername, "test_user")
	t.Setenv(EnvDatabasePassword, "test_password")
	t.Setenv(EnvDatabaseName, "test_db")
	t.Setenv(EnvMigrationsEnabled, "true")
}
