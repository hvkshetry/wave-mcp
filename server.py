#!/usr/bin/env python3
"""Wave Accounting MCP Server — 6 parameterized tools.

Dual transport: STDIO (default) or SSE (pass 'sse' argument).
Uses Wave GraphQL API with Full Access token (no OAuth flow).
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

mcp = FastMCP("wave_mcp")

# ── Lazy client singleton ────────────────────────────────────────────

_client = None
_client_lock = asyncio.Lock()


async def get_client():
    """Get or create the Wave client singleton."""
    global _client
    if _client is not None:
        return _client
    async with _client_lock:
        if _client is not None:
            return _client
        from client import WaveClient

        _client = WaveClient()
        logger.info("Wave client initialized")
    return _client


def _json(data: Any) -> str:
    """Serialize response data to JSON string."""
    return json.dumps(data, indent=2, default=str)


def _error(e: Exception, context: str = "") -> str:
    """Format an actionable error message for the agent."""
    msg = str(e)
    if "401" in msg or "authorization" in msg.lower() or "unauthorized" in msg.lower():
        hint = "Authentication failed. Check WAVE_ACCESS_TOKEN in .env."
    elif "404" in msg or "not found" in msg.lower():
        hint = "Entity not found. Use the list/search operation first to find valid IDs. Wave uses base64-encoded string IDs."
    elif "429" in msg or "rate" in msg.lower():
        hint = "Rate limited by Wave API (60 req/min). Wait a moment and retry."
    elif "didSucceed" in msg or "inputErrors" in msg:
        hint = "Mutation failed. Check required fields and data format."
    elif "WAVE_ACCESS_TOKEN" in msg or "WAVE_BUSINESS_ID" in msg:
        hint = "Run `uv run python auth_flow.py` to complete setup."
    elif "vendor" in msg.lower() and ("create" in msg.lower() or "update" in msg.lower() or "delete" in msg.lower()):
        hint = "Vendors are read-only in the Wave API. Only list, get, and search are supported."
    else:
        hint = "Check the operation name, entity_type, and parameters."
    prefix = f"[{context}] " if context else ""
    logger.error(f"{prefix}{msg}")
    return _json({"error": f"{prefix}{msg}", "hint": hint})


def _safe(tool_name: str):
    """Decorator to add error handling to tool functions."""
    import functools

    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                op = kwargs.get("operation", args[0] if args else "unknown")
                return _error(e, f"{tool_name}.{op}")
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════════════════
# Tool Definitions — 6 parameterized tools
# ═══════════════════════════════════════════════════════════════════════


@mcp.tool(
    annotations={
        "title": "Wave Chart of Accounts",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("account")
async def account(
    operation: str,
    account_id: str = None,
    data: dict = None,
    query: str = None,
    types: list[str] = None,
    subtypes: list[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Chart of Accounts management in Wave.

    Operations:
      Read: list, get, search
      Write: create, update, archive

    Args:
        operation: One of the operations listed above.
        account_id: Account ID (base64 string, required for get/update/archive).
        data: Dict of fields for create/update. Key fields:
            - name (str, required for create)
            - subtype (str, AccountSubtypeValue enum)
            - currency (str, currency code)
            - description (str)
        query: Search text for search operation (matches name, client-side).
        types: Filter by AccountTypeValue list (e.g. ["ASSET", "LIABILITY"]).
        subtypes: Filter by AccountSubtypeValue list.
        page: Page number (default 1).
        page_size: Results per page (default 50).

    Returns:
        JSON string with account data or {"error": "..."}.
    """
    c = await get_client()

    if operation == "list":
        return _json(await c.account_list(page, page_size, types, subtypes))

    elif operation == "get":
        return _json(await c.account_get(account_id))

    elif operation == "create":
        return _json(await c.account_create(data or {}))

    elif operation == "update":
        return _json(await c.account_update(account_id, data or {}))

    elif operation == "archive":
        return _json(await c.account_archive(account_id))

    elif operation == "search":
        return _json(await c.account_search(query or ""))

    else:
        return _json({"error": f"Unknown operation: {operation}. Valid: list, get, create, update, archive, search"})


@mcp.tool(
    annotations={
        "title": "Wave Customers & Vendors",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("party")
async def party(
    operation: str,
    party_type: str = "customer",
    party_id: str = None,
    data: dict = None,
    query: str = None,
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Customer and vendor management in Wave.

    NOTE: Vendors are read-only in the Wave API (no create/update/delete mutations).

    Operations:
      Read: list, get, search (both customer and vendor)
      Write: create, update, delete (customer only)

    Args:
        operation: One of the operations listed above.
        party_type: One of: customer, vendor.
        party_id: Entity ID (base64 string, required for get/update/delete).
        data: Dict of fields for create/update. Key customer fields:
            - name (str, required for create)
            - firstName, lastName (str)
            - email (str)
            - phone (str)
            - address (dict: addressLine1, addressLine2, city, postalCode, countryCode, provinceCode)
            - currency (str, currency code)
            - shippingDetails (dict: name, phone, address)
        query: Search text for search operation (matches name, client-side).
        page: Page number (default 1).
        page_size: Results per page (default 50).

    Returns:
        JSON string with party data or {"error": "..."}.
    """
    c = await get_client()

    if party_type not in ("customer", "vendor"):
        return _json({"error": f"Invalid party_type: {party_type}. Must be customer or vendor."})

    # Vendor write guard
    if party_type == "vendor" and operation in ("create", "update", "delete"):
        return _json({
            "error": f"Vendors are read-only in the Wave API. Operation '{operation}' is not available for vendors.",
            "hint": "Only list, get, and search operations are supported for vendors.",
        })

    if operation == "list":
        if party_type == "customer":
            return _json(await c.customer_list(page, page_size))
        else:
            return _json(await c.vendor_list(page, page_size))

    elif operation == "get":
        if party_type == "customer":
            return _json(await c.customer_get(party_id))
        else:
            return _json(await c.vendor_get(party_id))

    elif operation == "create":
        return _json(await c.customer_create(data or {}))

    elif operation == "update":
        return _json(await c.customer_update(party_id, data or {}))

    elif operation == "delete":
        return _json(await c.customer_delete(party_id))

    elif operation == "search":
        if party_type == "customer":
            return _json(await c.customer_search(query or ""))
        else:
            return _json(await c.vendor_search(query or ""))

    else:
        return _json({"error": f"Unknown operation: {operation}. Valid: list, get, create, update, delete, search"})


@mcp.tool(
    annotations={
        "title": "Wave Transactions",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("transaction")
async def transaction(
    operation: str,
    entity_type: str = "invoice",
    entity_id: str = None,
    data: dict = None,
    query: str = None,
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Invoice and money transaction management in Wave.

    Entity types: invoice, money_transaction

    Invoice operations:
      Read: list, get, search
      Write: create, update, delete, clone, send, approve, mark_sent, create_payment

    Money transaction operations:
      Write: create, bulk_create (BETA — create only, no list/get/update/delete)

    Args:
        operation: One of the operations listed above.
        entity_type: One of: invoice, money_transaction.
        entity_id: Transaction ID (base64 string, required for get/update/delete/clone/send/approve/mark_sent).
        data: Dict of fields for create/update.
            Invoice create: customerId, items (list of {productId, description, quantity, unitPrice, accountId}),
                invoiceDate, dueDate, memo, currency
            Invoice create_payment: invoiceId, paymentAccountId, amount, paymentDate,
                paymentMethod (BANK_TRANSFER|CASH|CHEQUE|CREDIT_CARD|OTHER|PAY_PAL), memo
            Money transaction create: anchor (dict with accountId, amount, direction: DEPOSIT|WITHDRAWAL),
                lineItems (list of {accountId, amount, balance}), externalId, description
            Money transaction bulk_create: transactions (list of individual create inputs)
        query: Search text for search operation (matches invoiceNumber, customer name, memo).
        page: Page number (default 1).
        page_size: Results per page (default 50).

    Returns:
        JSON string with transaction data or {"error": "..."}.
    """
    c = await get_client()

    INVOICE_OPS = {"list", "get", "create", "update", "delete", "clone", "send", "approve", "mark_sent", "create_payment", "search"}
    MONEY_TX_OPS = {"create", "bulk_create"}

    if entity_type == "invoice":
        if operation not in INVOICE_OPS:
            return _json({"error": f"Unknown invoice operation: {operation}. Valid: {', '.join(sorted(INVOICE_OPS))}"})

        if operation == "list":
            return _json(await c.invoice_list(page, page_size))
        elif operation == "get":
            return _json(await c.invoice_get(entity_id))
        elif operation == "create":
            return _json(await c.invoice_create(data or {}))
        elif operation == "update":
            return _json(await c.invoice_update(entity_id, data or {}))
        elif operation == "delete":
            return _json(await c.invoice_delete(entity_id))
        elif operation == "clone":
            return _json(await c.invoice_clone(entity_id))
        elif operation == "send":
            return _json(await c.invoice_send(entity_id))
        elif operation == "approve":
            return _json(await c.invoice_approve(entity_id))
        elif operation == "mark_sent":
            return _json(await c.invoice_mark_sent(entity_id))
        elif operation == "create_payment":
            return _json(await c.invoice_payment_create(data or {}))
        elif operation == "search":
            return _json(await c.invoice_search(query or ""))

    elif entity_type == "money_transaction":
        if operation not in MONEY_TX_OPS:
            return _json({
                "error": f"Money transactions only support 'create' and 'bulk_create' (BETA). Got: {operation}",
                "hint": "Money transactions are create-only in the Wave API. No list/get/update/delete available.",
            })
        if operation == "create":
            return _json(await c.money_transaction_create(data or {}))
        elif operation == "bulk_create":
            return _json(await c.money_transactions_create(data or {}))

    else:
        return _json({"error": f"Unknown entity_type: {entity_type}. Valid: invoice, money_transaction"})


@mcp.tool(
    annotations={
        "title": "Wave Products & Services",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("item")
async def item(
    operation: str,
    item_id: str = None,
    data: dict = None,
    query: str = None,
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Products & services management in Wave.

    Operations:
      Read: list, get, search
      Write: create, update, archive

    Args:
        operation: One of the operations listed above.
        item_id: Product ID (base64 string, required for get/update/archive).
        data: Dict of fields for create/update. Key fields:
            - name (str, required for create)
            - description (str)
            - unitPrice (decimal as string, e.g. "19.99")
            - isSold (bool)
            - isBought (bool)
            - incomeAccountId (str, account ID)
            - expenseAccountId (str, account ID)
            - defaultSalesTaxIds (list of tax IDs)
        query: Search text for search operation (matches name, client-side).
        page: Page number (default 1).
        page_size: Results per page (default 50).

    Returns:
        JSON string with product data or {"error": "..."}.
    """
    c = await get_client()

    if operation == "list":
        return _json(await c.product_list(page, page_size))

    elif operation == "get":
        return _json(await c.product_get(item_id))

    elif operation == "create":
        return _json(await c.product_create(data or {}))

    elif operation == "update":
        return _json(await c.product_update(item_id, data or {}))

    elif operation == "archive":
        return _json(await c.product_archive(item_id))

    elif operation == "search":
        return _json(await c.product_search(query or ""))

    else:
        return _json({"error": f"Unknown operation: {operation}. Valid: list, get, create, update, archive, search"})


@mcp.tool(
    annotations={
        "title": "Wave Reference Data & Settings",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
@_safe("reference")
async def reference(
    operation: str,
    entity_id: str = None,
    data: dict = None,
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Lookup data, settings, sales taxes, and estimates in Wave.

    Read operations:
      list_businesses, get_business, get_user,
      list_currencies, list_countries, list_account_types, list_account_subtypes,
      list_sales_taxes, get_sales_tax, list_estimates, get_estimate

    Write operations (sales taxes only):
      create_sales_tax, update_sales_tax, archive_sales_tax

    Args:
        operation: One of the operations listed above.
        entity_id: Entity ID (base64 string, for get_sales_tax/update_sales_tax/archive_sales_tax/get_estimate).
        data: Dict of fields for create/update sales tax:
            - name (str, required for create)
            - abbreviation (str)
            - rate (str, decimal percentage e.g. "13.0")
            - isCompound (bool)
            - isRecoverable (bool)
        page: Page number (for list_estimates, default 1).
        page_size: Results per page (for list_estimates, default 50).

    Returns:
        JSON string with reference data or {"error": "..."}.
    """
    c = await get_client()

    # Read operations
    if operation == "list_businesses":
        return _json(await c.businesses_list())
    elif operation == "get_business":
        return _json(await c.business_get())
    elif operation == "get_user":
        return _json(await c.user_get())
    elif operation == "list_currencies":
        return _json(await c.currencies_list())
    elif operation == "list_countries":
        return _json(await c.countries_list())
    elif operation == "list_account_types":
        return _json(await c.account_types_list())
    elif operation == "list_account_subtypes":
        return _json(await c.account_subtypes_list())

    # Sales tax operations
    elif operation == "list_sales_taxes":
        return _json(await c.sales_tax_list())
    elif operation == "get_sales_tax":
        return _json(await c.sales_tax_get(entity_id))
    elif operation == "create_sales_tax":
        return _json(await c.sales_tax_create(data or {}))
    elif operation == "update_sales_tax":
        return _json(await c.sales_tax_update(entity_id, data or {}))
    elif operation == "archive_sales_tax":
        return _json(await c.sales_tax_archive(entity_id))

    # Estimate operations (read-only)
    elif operation == "list_estimates":
        return _json(await c.estimate_list(page, page_size))
    elif operation == "get_estimate":
        return _json(await c.estimate_get(entity_id))

    else:
        valid = (
            "list_businesses, get_business, get_user, list_currencies, list_countries, "
            "list_account_types, list_account_subtypes, list_sales_taxes, get_sales_tax, "
            "create_sales_tax, update_sales_tax, archive_sales_tax, list_estimates, get_estimate"
        )
        return _json({"error": f"Unknown operation: {operation}. Valid: {valid}"})


@mcp.tool(
    annotations={
        "title": "Wave Synthesized Reports",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
@_safe("report")
async def report(
    operation: str,
) -> str:
    """Synthesized financial summaries from Wave data.

    Wave has no report API. These are built from raw account/customer data.

    Operations:
      business_summary — Business info + accounts grouped by type with counts
      account_balances — All accounts with name, type, subtype, and balance info
      customer_balances — All customers with outstanding and overdue amounts

    Args:
        operation: One of the operations listed above.

    Returns:
        JSON string with synthesized report data or {"error": "..."}.
    """
    c = await get_client()

    if operation == "business_summary":
        return _json(await c.report_business_summary())

    elif operation == "account_balances":
        return _json(await c.report_account_balances())

    elif operation == "customer_balances":
        return _json(await c.report_customer_balances())

    else:
        return _json({"error": f"Unknown operation: {operation}. Valid: business_summary, account_balances, customer_balances"})


# ═══════════════════════════════════════════════════════════════════════
# Entry point — dual transport
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    transport = sys.argv[1] if len(sys.argv) > 1 else "stdio"

    if transport == "sse":
        port = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("PORT", "3076"))
        logger.info(f"Starting Wave MCP Server on SSE port {port}")
        mcp.run(transport="sse", port=port)
    else:
        logger.info("Starting Wave MCP Server on STDIO")
        mcp.run(transport="stdio")
