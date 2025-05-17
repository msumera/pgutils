
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