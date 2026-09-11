
# Go PgUtils

## Environment variables
| Variable                | Default value    | Description                                                                                            |
|-------------------------|------------------|--------------------------------------------------------------------------------------------------------|
| DB_ADDRESS              | `localhost:5432` | Host and port of the PostgreSQL database server                                                        |
| DB_USERNAME             | `postgres`       | Username used to connect to the database                                                               |
| DB_PASSWORD             | `postgres`       | Password used to authenticate the database user                                                        |
| DB_NAME                 | `postgres`       | Name of the target PostgreSQL database                                                                 |
| DB_SCHEMA               |                  | Schema to use for queries (empty = `public`)                                                           |
| DB_MIGRATION_SCHEMA     |                  | Schema where migration history will be recorded (empty = `public`)                                     |
| DB_SSL_MODE             | `disable`        | SSL mode for database connection (`disable`, `require`, `verify-full`, `verify-ca`, `prefer`, `allow`) |
| DB_SSL_ROOT_CERT        |                  | Path to the SSL root certificate file (used with SSL modes that verify)                                |
| DB_SSL_CERT             |                  | Path to the client SSL certificate file                                                                |
| DB_SSL_KEY              |                  | Path to the client SSL private key file                                                                |
| DB_MIGRATIONS_ENABLED   | `true`           | Whether to automatically apply migrations at startup (`true`/`false`)                                  |
| DB_CHANGELOG_SCHEMA     | `public`         | Schema where the migration changelog table is stored                                                   |
| DB_CHANGELOG_TABLE      | `changelog`      | Name of the table used to track applied migrations                                                     |
| DB_MIGRATIONS_DIRECTORY | `db`             | Directory containing `.sql` migration files                                                            |
| DB_MIN_CONNS            | `0`              | Minimum number of connections in the pool                                                              |
| DB_MAX_CONNS            | `0`              | Maximum number of connections in the pool                                                              |
| DB_MAX_CONN_LIFETIME    | `0`              | Maximum lifetime duration for connections (e.g., `1h`, `30m`)                                          |
| DB_MAX_CONN_IDLE_TIME   | `0`              | Maximum idle duration for connections before closure (e.g., `15m`, `5m`)                               |
| DB_HEALTH_CHECK_PERIOD  | `0`              | Frequency of background health checks on pool connections (e.g., `30s`, `1m`)                          |

## Features and Usage

### Transactions (`DoInTransaction`, `DoInTransactionWithOpts`)
Execute business operations safely wrapped in a database transaction with automatic rollback on error or panic:

```go
// Standard transaction
result, err := pg.DoInTransaction(ctx, pool, func(tx pgx.Tx) (string, error) {
    // ...
    return "ok", nil
})

// Transaction with custom options (e.g., ReadOnly, Serializable)
err := pg.DoInTransactionNoResultWithOpts(ctx, pool, pgx.TxOptions{
    AccessMode: pgx.ReadOnly,
    IsoLevel:   pgx.RepeatableRead,
}, func(tx pgx.Tx) error {
    // Read operations
    return nil
})
```

### Embedded Migrations (`embed.FS` / `fs.FS`)
You can embed SQL migration files directly into your binary using Go `embed`:

```go
import (
    "embed"
    "github.com/msumera/pgutils"
)

//go:embed db/*.sql
var migrationFiles embed.FS

// Option A: Configure during pool creation
cfg := pg.CreateConfigurationFromEnv()
cfg.MigrationsFS = migrationFiles
pool, err := pg.ConnectWithConfigContext(ctx, cfg)

// Option B: Run migrations explicitly on an existing pool
err := pg.MigrateFS(ctx, pool, migrationFiles, cfg)
```

### Health Check & Ping
Easily check database connectivity and inspect pool statistics:

```go
if err := pg.Ping(ctx, pool); err != nil {
    log.Fatal("database unreachable:", err)
}

stat, err := pg.HealthCheck(ctx, pool)
if err == nil {
    fmt.Printf("Total conns: %d, Idle conns: %d\n", stat.TotalConns(), stat.IdleConns())
}
```

## Migration File Ordering Guide

When applying SQL migration files, the order in which the files are executed is critical. This guide explains how migration files are sorted and executed based on their filenames.

### Filename Format

Migration files are expected to follow this naming pattern: `<version_id>_<description>.sql`

- `<version_id>` consists of numeric parts separated by underscores (e.g., `0`, `0_1`, `1`, `2_3_4`, etc.).
- `<description>` is a human-readable string that describes the purpose of the migration (e.g., `init`, `addcolumn`, etc.).

Only the numeric `version_id` is used for ordering migrations.

### Ordering Rules

1. **Version ID Parsing**  
   The numeric prefix is parsed as a sequence of integers split by underscores (`_`).
    - Examples:
        - `0_init.sql` → ID: `(0)`
        - `0_1_init_data.sql` → ID: `(0, 1)`
        - `1_addcolumn.sql` → ID: `(1)`

2. **Lexicographic Comparison of Numeric Tuples**  
   Version IDs are compared in lexicographic order: `(0) < (0, 1) < (1)`

3. **Files with the Same ID**  
   If multiple files share the same numeric ID, their full filenames are compared alphabetically as a tie-breaker.

### Example Ordering

Given the following files:

- `0_1_init_data.sql`
- `0_init.sql`
- `1_addcolumn.sql`

They are ordered as:

1. `0_init.sql` → ID: `(0)`
2. `0_1_init_data.sql` → ID: `(0, 1)`
3. `1_addcolumn.sql` → ID: `(1)`

## Notes

- Always ensure your migration filenames have a clear numeric prefix to ensure correct execution order.
- Avoid using non-numeric prefixes or inconsistent patterns, as these may be ignored or cause sorting issues.