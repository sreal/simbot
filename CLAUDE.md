# Simbot Project Instructions

## Running Tests

```bash
./scripts/test.sh                    # Run all tests
./scripts/test.sh tests/test_blob_tools.py   # Run specific test file
```

## Project Structure

- `src/simbot/sql_tools/` - SQL query execution engine
- `src/simbot/blob_tools/` - Blob storage check engine
- `src/simbot/interfaces/slack/` - Slack bot interface
- `src/simbot/interfaces/mcp/` - MCP server interface
- `queries/` - YAML query/check definitions

## Adding New Tools

### SQL Queries
Create a YAML file in `queries/` (e.g., `lookup_*.yaml`):
```yaml
name: "Query Name"
trigger: "lookup something"
database: "dbname"
sql: "SELECT * FROM table WHERE col = ?"
parameters:
  - name: param_name
    type: string
```

Use `bind_from` when one user input needs to fill multiple `?` placeholders.
The derived param is invisible to Slack and MCP — the user supplies the
source param once:
```yaml
sql: "SELECT * FROM t WHERE name LIKE '%' + ? + '%' OR id = ?"
parameters:
  - name: Search
    type: string
    required: true
  - name: SearchExact
    type: string
    required: true
    bind_from: Search
```

### Blob Checks
Create a YAML file in `queries/` starting with `blob_` (e.g., `blob_check_*.yaml`):
```yaml
name: "Check Name"
trigger: "check something"
storage:
  provider: "azure"
  account_env_key: "AZURE_STORAGE_ACCOUNT"
  key_env_key: "AZURE_STORAGE_KEY"
  container: "container-name"
files:
  - name: file_name
    path: "{param}/file.ext"
    return: metadata
```

## Environment Variables

See `.env.example` for required configuration.
