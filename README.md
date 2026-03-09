# Wave MCP Server

MCP server for [Wave](https://www.waveapps.com/) accounting — 6 tools, 52 operations covering the full Wave GraphQL API.

Built with [FastMCP](https://github.com/jlowin/fastmcp) and [httpx](https://www.python-httpx.org/). Mirrors the architecture of [`quickbooks-mcp`](https://github.com/hvkshetry/quickbooks-mcp).

## Quick Start

```bash
# Install dependencies
uv sync

# Configure credentials
cp .env.example .env
# Edit .env — add your WAVE_ACCESS_TOKEN from Wave > Settings > API Applications

# Discover your business ID
uv run python auth_flow.py

# Run (STDIO for Claude Desktop / MCP clients)
uv run python server.py

# Run (SSE on port 3076)
uv run python server.py sse
```

## Configuration

| Variable | Description |
|----------|-------------|
| `WAVE_ACCESS_TOKEN` | Full Access token from Wave app settings |
| `WAVE_BUSINESS_ID` | Set automatically by `auth_flow.py`, or manually |

## Tools & Operations

### `account` — Chart of Accounts
| Operation | Description |
|-----------|-------------|
| `list` | List accounts with optional type/subtype filters, pagination |
| `get` | Get account by ID |
| `create` | Create account (name, subtype, currency, description) |
| `update` | Update account fields |
| `archive` | Archive an account |
| `search` | Client-side name search |

### `party` — Customers & Vendors
| Operation | Customer | Vendor |
|-----------|----------|--------|
| `list` | Yes | Yes |
| `get` | Yes | Yes |
| `create` | Yes | **Read-only** |
| `update` | Yes | **Read-only** |
| `delete` | Yes | **Read-only** |
| `search` | Yes | Yes |

### `transaction` — Invoices & Money Transactions
**Invoice operations:** `list`, `get`, `create`, `update`, `delete`, `clone`, `send`, `approve`, `mark_sent`, `create_payment`, `search`

**Money transaction operations:** `create`, `bulk_create` (BETA)

### `item` — Products & Services
`list`, `get`, `create`, `update`, `archive`, `search`

### `reference` — Lookup Data & Settings
`list_businesses`, `get_business`, `get_user`, `list_currencies`, `list_countries`, `list_account_types`, `list_account_subtypes`, `list_sales_taxes`, `get_sales_tax`, `create_sales_tax`, `update_sales_tax`, `archive_sales_tax`, `list_estimates`, `get_estimate`

### `report` — Synthesized Summaries
| Operation | Description |
|-----------|-------------|
| `business_summary` | Business info + accounts grouped by type with counts |
| `account_balances` | All accounts with name, type, balance, balanceInBusinessCurrency |
| `customer_balances` | All customers with outstanding and overdue amounts |

## MCP Client Configuration

### Claude Desktop / Claude Code (STDIO)
```json
{
  "mcpServers": {
    "wave": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/wave-mcp", "python", "server.py"]
    }
  }
}
```

### SSE (remote / multi-client)
```json
{
  "mcpServers": {
    "wave": {
      "url": "http://localhost:3076/sse"
    }
  }
}
```

## Key Differences from QuickBooks MCP

| | Wave | QuickBooks |
|-|------|-----------|
| **API** | GraphQL | REST |
| **Auth** | Static Bearer token | OAuth 2.0 + token refresh |
| **IDs** | Base64 strings | Integers |
| **Pagination** | Page-based (page/pageSize) | Offset-based (start_position) |
| **Search** | Client-side name filtering | Server-side WHERE queries |
| **Vendors** | Read-only | Full CRUD |
| **Reports** | Synthesized from account data | Native report API |
| **Money Transactions** | Create-only (BETA) | N/A |

## Known Limitations

- **Vendors are read-only** — no create/update/delete mutations in the Wave API
- **Money transactions are create-only** (BETA) — no list/get/update/delete
- **No server-side text search** — client-side filtering, limited to first ~250 results per query
- **No financial reports API** — synthesized account balance summaries only
- **Estimates are read-only** — no create/update mutations
- **Rate limit**: 60 requests/minute (automatic exponential backoff on 429)
