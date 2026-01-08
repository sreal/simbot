# simbot

SQL tools bot for Slack and MCP.

## Setup

```bash
cp .env.example .env
# Configure tokens and connection strings

docker-compose up
```

## Security

- `.env` and any private query definitions are intentionally excluded from version control by `.gitignore`.
- Never commit real tokens, passwords, or internal hostnames.
- Use only `.env.example` as a reference with placeholders.

## Query Definition

Queries are YAML files mounted via Docker volume (`./queries:/app/queries:ro`).

```yaml
name: "Account Lookup"
description: "Find account by name"
trigger: "account lookup"
enabled: true
database: "exampledb"
credentials_env_key: "DBCONN_DEFAULT"
sql: |
  SELECT * FROM accounts WHERE name LIKE '%' + ? + '%'
parameters:
  - name: AccountName
    type: string
    required: true
cache_ttl_seconds: 3600

# Optional MCP overrides
mcp:
  name: "account_lookup"
  group: "account_tools"
  description: "Extended description for Claude"
```

## Slack Commands

- `@bot queries` - List queries
- `@bot account lookup <name>` - Run query
- `@bot clear cache all` - Clear cache
- `@bot reload queries` - Reload definitions

## MCP Server

Add to Claude Desktop config (`~/.config/claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "simbot": {
      "command": "uv",
      "args": ["run", "simbot-mcp"],
      "cwd": "/path/to/simbot"
    }
  }
}
```

## Development

```bash
uv sync --extra dev
uv run pytest -v
```
