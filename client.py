"""Wave accounting GraphQL client — async httpx with backoff and business ID injection.

No OAuth flow or token refresh needed (Full Access token).
All queries use relay-style pagination (page/pageSize) with edges/node patterns.
"""

import asyncio
import logging
import os

import httpx

logger = logging.getLogger(__name__)

# ── GraphQL Query Templates ──────────────────────────────────────────

# Field fragments reused across queries
ACCOUNT_FIELDS = """
    id
    name
    description
    displayId
    currency { code }
    type { name value }
    subtype { name value }
    normalBalanceType
    isArchived
    sequence
    balance
    balanceInBusinessCurrency
"""

CUSTOMER_FIELDS = """
    id
    name
    firstName
    lastName
    email
    phone
    address {
        addressLine1
        addressLine2
        city
        province { code name }
        country { code name }
        postalCode
    }
    currency { code }
    shippingDetails {
        name
        phone
        address {
            addressLine1
            addressLine2
            city
            province { code name }
            country { code name }
            postalCode
        }
    }
    outstandingAmount { value currency { code } }
    overdueAmount { value currency { code } }
    createdAt
    modifiedAt
"""

VENDOR_FIELDS = """
    id
    name
    firstName
    lastName
    email
    phone
    address {
        addressLine1
        addressLine2
        city
        province { code name }
        country { code name }
        postalCode
    }
    currency { code }
    shippingDetails {
        name
        phone
        address {
            addressLine1
            addressLine2
            city
            province { code name }
            country { code name }
            postalCode
        }
    }
    createdAt
    modifiedAt
"""

INVOICE_FIELDS = """
    id
    status
    invoiceNumber
    invoiceDate
    dueDate
    customer { id name }
    items {
        description
        quantity
        unitPrice
        subtotal { value currency { code } }
        total { value currency { code } }
        product { id name }
        account { id name }
        taxes { salesTax { id name } amount { value currency { code } } }
    }
    amountDue { value currency { code } }
    amountPaid { value currency { code } }
    taxTotal { value currency { code } }
    subtotal { value currency { code } }
    total { value currency { code } }
    currency { code }
    memo
    footer
    pdfUrl
    viewUrl
    lastSentAt
    lastSentVia
    createdAt
    modifiedAt
"""

PRODUCT_FIELDS = """
    id
    name
    description
    unitPrice
    isSold
    isBought
    isArchived
    incomeAccount { id name }
    expenseAccount { id name }
    defaultSalesTaxes { id name }
    createdAt
    modifiedAt
"""

SALES_TAX_FIELDS = """
    id
    name
    abbreviation
    rate
    isCompound
    isRecoverable
    isArchived
    createdAt
    modifiedAt
"""

ESTIMATE_FIELDS = """
    id
    estimateNumber
    estimateDate
    dueDate
    customer { id name }
    amountDue { value currency { code } }
    amountPaid { value currency { code } }
    taxTotal { value currency { code } }
    subtotal { value currency { code } }
    total { value currency { code } }
    currency { code }
    memo
    footer
    pdfUrl
    viewUrl
    lastSentAt
    lastViewedAt
    createdAt
    modifiedAt
"""

BUSINESS_FIELDS = """
    id
    name
    isPersonal
    currency { code }
    type { name value }
    subtype { name value }
    address {
        addressLine1
        addressLine2
        city
        province { code name }
        country { code name }
        postalCode
    }
    phone
    fax
    mobile
    tollFree
    website
    timezone
    isArchived
    createdAt
    modifiedAt
"""

USER_FIELDS = """
    id
    defaultEmail
    firstName
    lastName
    createdAt
    modifiedAt
"""

# ── Queries ──────────────────────────────────────────────────────────

QUERIES = {
    # Account queries
    "account_list": """
        query($businessId: ID!, $page: Int!, $pageSize: Int!) {
            business(id: $businessId) {
                accounts(page: $page, pageSize: $pageSize) {
                    pageInfo { currentPage totalPages totalCount }
                    edges { node {""" + ACCOUNT_FIELDS + """} }
                }
            }
        }
    """,
    "account_list_filtered": """
        query($businessId: ID!, $page: Int!, $pageSize: Int!, $types: [AccountTypeValue!], $subtypes: [AccountSubtypeValue!]) {
            business(id: $businessId) {
                accounts(page: $page, pageSize: $pageSize, types: $types, subtypes: $subtypes) {
                    pageInfo { currentPage totalPages totalCount }
                    edges { node {""" + ACCOUNT_FIELDS + """} }
                }
            }
        }
    """,
    "account_get": """
        query($businessId: ID!, $accountId: ID!) {
            business(id: $businessId) {
                account(id: $accountId) {""" + ACCOUNT_FIELDS + """}
            }
        }
    """,

    # Customer queries
    "customer_list": """
        query($businessId: ID!, $page: Int!, $pageSize: Int!) {
            business(id: $businessId) {
                customers(page: $page, pageSize: $pageSize) {
                    pageInfo { currentPage totalPages totalCount }
                    edges { node {""" + CUSTOMER_FIELDS + """} }
                }
            }
        }
    """,
    "customer_get": """
        query($businessId: ID!, $customerId: ID!) {
            business(id: $businessId) {
                customer(id: $customerId) {""" + CUSTOMER_FIELDS + """}
            }
        }
    """,

    # Vendor queries
    "vendor_list": """
        query($businessId: ID!, $page: Int!, $pageSize: Int!) {
            business(id: $businessId) {
                vendors(page: $page, pageSize: $pageSize) {
                    pageInfo { currentPage totalPages totalCount }
                    edges { node {""" + VENDOR_FIELDS + """} }
                }
            }
        }
    """,
    "vendor_get": """
        query($businessId: ID!, $vendorId: ID!) {
            business(id: $businessId) {
                vendor(id: $vendorId) {""" + VENDOR_FIELDS + """}
            }
        }
    """,

    # Invoice queries
    "invoice_list": """
        query($businessId: ID!, $page: Int!, $pageSize: Int!) {
            business(id: $businessId) {
                invoices(page: $page, pageSize: $pageSize) {
                    pageInfo { currentPage totalPages totalCount }
                    edges { node {""" + INVOICE_FIELDS + """} }
                }
            }
        }
    """,
    "invoice_get": """
        query($businessId: ID!, $invoiceId: ID!) {
            business(id: $businessId) {
                invoice(id: $invoiceId) {""" + INVOICE_FIELDS + """}
            }
        }
    """,

    # Product queries
    "product_list": """
        query($businessId: ID!, $page: Int!, $pageSize: Int!) {
            business(id: $businessId) {
                products(page: $page, pageSize: $pageSize) {
                    pageInfo { currentPage totalPages totalCount }
                    edges { node {""" + PRODUCT_FIELDS + """} }
                }
            }
        }
    """,
    "product_get": """
        query($businessId: ID!, $productId: ID!) {
            business(id: $businessId) {
                product(id: $productId) {""" + PRODUCT_FIELDS + """}
            }
        }
    """,

    # Sales tax queries
    "sales_tax_list": """
        query($businessId: ID!) {
            business(id: $businessId) {
                salesTaxes {
                    edges { node {""" + SALES_TAX_FIELDS + """} }
                }
            }
        }
    """,
    "sales_tax_get": """
        query($businessId: ID!, $salesTaxId: ID!) {
            business(id: $businessId) {
                salesTax(id: $salesTaxId) {""" + SALES_TAX_FIELDS + """}
            }
        }
    """,

    # Estimate queries
    "estimate_list": """
        query($businessId: ID!, $page: Int!, $pageSize: Int!) {
            business(id: $businessId) {
                estimates(page: $page, pageSize: $pageSize) {
                    pageInfo { currentPage totalPages totalCount }
                    edges { node {""" + ESTIMATE_FIELDS + """} }
                }
            }
        }
    """,
    "estimate_get": """
        query($businessId: ID!, $estimateId: ID!) {
            business(id: $businessId) {
                estimate(id: $estimateId) {""" + ESTIMATE_FIELDS + """}
            }
        }
    """,

    # Reference queries
    "businesses_list": """
        query {
            businesses {
                edges { node {""" + BUSINESS_FIELDS + """} }
            }
        }
    """,
    "business_get": """
        query($businessId: ID!) {
            business(id: $businessId) {""" + BUSINESS_FIELDS + """}
        }
    """,
    "user_get": """
        query {
            user {""" + USER_FIELDS + """}
        }
    """,
    "currencies_list": """
        query {
            currencies {
                code
                symbol
                name
                plural
                exponent
            }
        }
    """,
    "countries_list": """
        query {
            countries {
                code
                name
                currency { code }
                provinces { code name }
            }
        }
    """,
    "account_types_list": """
        query {
            accountTypes {
                name
                value
                normalBalanceType
            }
        }
    """,
    "account_subtypes_list": """
        query {
            accountSubtypes {
                name
                value
                type { name value }
            }
        }
    """,
}

# ── Mutations ────────────────────────────────────────────────────────

MUTATIONS = {
    # Account mutations
    "account_create": """
        mutation($input: AccountCreateInput!) {
            accountCreate(input: $input) {
                didSucceed
                inputErrors { path message code }
                account {""" + ACCOUNT_FIELDS + """}
            }
        }
    """,
    "account_patch": """
        mutation($input: AccountPatchInput!) {
            accountPatch(input: $input) {
                didSucceed
                inputErrors { path message code }
                account {""" + ACCOUNT_FIELDS + """}
            }
        }
    """,
    "account_archive": """
        mutation($input: AccountArchiveInput!) {
            accountArchive(input: $input) {
                didSucceed
                inputErrors { path message code }
                account {""" + ACCOUNT_FIELDS + """}
            }
        }
    """,

    # Customer mutations
    "customer_create": """
        mutation($input: CustomerCreateInput!) {
            customerCreate(input: $input) {
                didSucceed
                inputErrors { path message code }
                customer {""" + CUSTOMER_FIELDS + """}
            }
        }
    """,
    "customer_patch": """
        mutation($input: CustomerPatchInput!) {
            customerPatch(input: $input) {
                didSucceed
                inputErrors { path message code }
                customer {""" + CUSTOMER_FIELDS + """}
            }
        }
    """,
    "customer_delete": """
        mutation($input: CustomerDeleteInput!) {
            customerDelete(input: $input) {
                didSucceed
                inputErrors { path message code }
            }
        }
    """,

    # Invoice mutations
    "invoice_create": """
        mutation($input: InvoiceCreateInput!) {
            invoiceCreate(input: $input) {
                didSucceed
                inputErrors { path message code }
                invoice {""" + INVOICE_FIELDS + """}
            }
        }
    """,
    "invoice_patch": """
        mutation($input: InvoicePatchInput!) {
            invoicePatch(input: $input) {
                didSucceed
                inputErrors { path message code }
                invoice {""" + INVOICE_FIELDS + """}
            }
        }
    """,
    "invoice_delete": """
        mutation($input: InvoiceDeleteInput!) {
            invoiceDelete(input: $input) {
                didSucceed
                inputErrors { path message code }
            }
        }
    """,
    "invoice_clone": """
        mutation($input: InvoiceCloneInput!) {
            invoiceClone(input: $input) {
                didSucceed
                inputErrors { path message code }
                invoice {""" + INVOICE_FIELDS + """}
            }
        }
    """,
    "invoice_send": """
        mutation($input: InvoiceSendInput!) {
            invoiceSend(input: $input) {
                didSucceed
                inputErrors { path message code }
            }
        }
    """,
    "invoice_approve": """
        mutation($input: InvoiceApproveInput!) {
            invoiceApprove(input: $input) {
                didSucceed
                inputErrors { path message code }
                invoice {""" + INVOICE_FIELDS + """}
            }
        }
    """,
    "invoice_mark_sent": """
        mutation($input: InvoiceMarkSentInput!) {
            invoiceMarkSent(input: $input) {
                didSucceed
                inputErrors { path message code }
                invoice {""" + INVOICE_FIELDS + """}
            }
        }
    """,

    # Product mutations
    "product_create": """
        mutation($input: ProductCreateInput!) {
            productCreate(input: $input) {
                didSucceed
                inputErrors { path message code }
                product {""" + PRODUCT_FIELDS + """}
            }
        }
    """,
    "product_patch": """
        mutation($input: ProductPatchInput!) {
            productPatch(input: $input) {
                didSucceed
                inputErrors { path message code }
                product {""" + PRODUCT_FIELDS + """}
            }
        }
    """,
    "product_archive": """
        mutation($input: ProductArchiveInput!) {
            productArchive(input: $input) {
                didSucceed
                inputErrors { path message code }
                product {""" + PRODUCT_FIELDS + """}
            }
        }
    """,

    # Sales tax mutations
    "sales_tax_create": """
        mutation($input: SalesTaxCreateInput!) {
            salesTaxCreate(input: $input) {
                didSucceed
                inputErrors { path message code }
                salesTax {""" + SALES_TAX_FIELDS + """}
            }
        }
    """,
    "sales_tax_patch": """
        mutation($input: SalesTaxPatchInput!) {
            salesTaxPatch(input: $input) {
                didSucceed
                inputErrors { path message code }
                salesTax {""" + SALES_TAX_FIELDS + """}
            }
        }
    """,
    "sales_tax_archive": """
        mutation($input: SalesTaxArchiveInput!) {
            salesTaxArchive(input: $input) {
                didSucceed
                inputErrors { path message code }
                salesTax {""" + SALES_TAX_FIELDS + """}
            }
        }
    """,

    # Invoice payment mutation
    "invoice_payment_create_manual": """
        mutation($input: InvoicePaymentCreateManualInput!) {
            invoicePaymentCreateManual(input: $input) {
                didSucceed
                inputErrors { path message code }
                invoicePayment {
                    id
                    paymentDate
                    amount
                    paymentMethod
                    memo
                    createdAt
                }
            }
        }
    """,

    # Money transaction mutations (BETA — create only)
    "money_transaction_create": """
        mutation($input: MoneyTransactionCreateInput!) {
            moneyTransactionCreate(input: $input) {
                didSucceed
                inputErrors { path message code }
                transaction { id }
            }
        }
    """,
    "money_transactions_create": """
        mutation($input: MoneyTransactionsCreateInput!) {
            moneyTransactionsCreate(input: $input) {
                didSucceed
                inputErrors { path message code }
                transactions { id }
            }
        }
    """,
}


# ── Wave Client ──────────────────────────────────────────────────────

class WaveClient:
    """Async Wave GraphQL client with rate-limit backoff."""

    GRAPHQL_URL = "https://gql.waveapps.com/graphql/public"

    def __init__(self):
        self.token = os.getenv("WAVE_ACCESS_TOKEN")
        self.business_id = os.getenv("WAVE_BUSINESS_ID")

        if not self.token:
            raise RuntimeError(
                "WAVE_ACCESS_TOKEN not set. Add it to .env from Wave > Settings > API Applications."
            )
        if not self.business_id:
            raise RuntimeError(
                "WAVE_BUSINESS_ID not set. Run `uv run python auth_flow.py` to discover it."
            )

        self._http = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )

    async def _execute(self, query: str, variables: dict | None = None) -> dict:
        """Execute a GraphQL query/mutation and return the data dict."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        resp = await self._http.post(self.GRAPHQL_URL, json=payload)
        resp.raise_for_status()
        body = resp.json()

        if "errors" in body:
            errors = body["errors"]
            msgs = "; ".join(e.get("message", str(e)) for e in errors)
            raise RuntimeError(f"GraphQL error: {msgs}")

        return body.get("data", {})

    async def _call_with_backoff(self, query: str, variables: dict | None = None) -> dict:
        """Execute with exponential backoff on 429 rate limits."""
        for attempt in range(4):
            try:
                return await self._execute(query, variables)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Rate limited (429), backing off {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                raise
        raise RuntimeError("Rate limit exceeded after 4 retries")

    def _extract_nodes(self, edges_data: list[dict]) -> list[dict]:
        """Extract node dicts from relay-style edges."""
        return [edge["node"] for edge in (edges_data or [])]

    def _check_mutation(self, result: dict, mutation_name: str) -> dict:
        """Check mutation result for didSucceed and inputErrors."""
        mutation_data = result.get(mutation_name, {})
        if not mutation_data.get("didSucceed", False):
            errors = mutation_data.get("inputErrors", [])
            if errors:
                msgs = "; ".join(f"{e.get('path','?')}: {e.get('message','?')}" for e in errors)
                raise RuntimeError(f"Mutation {mutation_name} failed: {msgs}")
            raise RuntimeError(f"Mutation {mutation_name} failed (no error details)")
        return mutation_data

    # ── Account operations ───────────────────────────────────────────

    async def account_list(self, page: int = 1, page_size: int = 50,
                           types: list[str] | None = None,
                           subtypes: list[str] | None = None) -> dict:
        variables = {"businessId": self.business_id, "page": page, "pageSize": page_size}
        if types or subtypes:
            if types:
                variables["types"] = types
            if subtypes:
                variables["subtypes"] = subtypes
            data = await self._call_with_backoff(QUERIES["account_list_filtered"], variables)
        else:
            data = await self._call_with_backoff(QUERIES["account_list"], variables)
        accounts_data = data["business"]["accounts"]
        return {
            "pageInfo": accounts_data["pageInfo"],
            "accounts": self._extract_nodes(accounts_data["edges"]),
        }

    async def account_get(self, account_id: str) -> dict:
        data = await self._call_with_backoff(
            QUERIES["account_get"],
            {"businessId": self.business_id, "accountId": account_id},
        )
        return data["business"]["account"]

    async def account_create(self, input_data: dict) -> dict:
        input_data["businessId"] = self.business_id
        data = await self._call_with_backoff(MUTATIONS["account_create"], {"input": input_data})
        result = self._check_mutation(data, "accountCreate")
        return result.get("account", {})

    async def account_update(self, account_id: str, input_data: dict) -> dict:
        input_data["id"] = account_id
        data = await self._call_with_backoff(MUTATIONS["account_patch"], {"input": input_data})
        result = self._check_mutation(data, "accountPatch")
        return result.get("account", {})

    async def account_archive(self, account_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["account_archive"],
            {"input": {"id": account_id}},
        )
        result = self._check_mutation(data, "accountArchive")
        return result.get("account", {})

    async def account_search(self, name_query: str, page_size: int = 250) -> list[dict]:
        """Client-side name search across all accounts (no server-side search)."""
        result = await self.account_list(page=1, page_size=page_size)
        q = name_query.lower()
        return [a for a in result["accounts"] if q in (a.get("name") or "").lower()]

    # ── Customer operations ──────────────────────────────────────────

    async def customer_list(self, page: int = 1, page_size: int = 50) -> dict:
        data = await self._call_with_backoff(
            QUERIES["customer_list"],
            {"businessId": self.business_id, "page": page, "pageSize": page_size},
        )
        customers_data = data["business"]["customers"]
        return {
            "pageInfo": customers_data["pageInfo"],
            "customers": self._extract_nodes(customers_data["edges"]),
        }

    async def customer_get(self, customer_id: str) -> dict:
        data = await self._call_with_backoff(
            QUERIES["customer_get"],
            {"businessId": self.business_id, "customerId": customer_id},
        )
        return data["business"]["customer"]

    async def customer_create(self, input_data: dict) -> dict:
        input_data["businessId"] = self.business_id
        data = await self._call_with_backoff(MUTATIONS["customer_create"], {"input": input_data})
        result = self._check_mutation(data, "customerCreate")
        return result.get("customer", {})

    async def customer_update(self, customer_id: str, input_data: dict) -> dict:
        input_data["id"] = customer_id
        data = await self._call_with_backoff(MUTATIONS["customer_patch"], {"input": input_data})
        result = self._check_mutation(data, "customerPatch")
        return result.get("customer", {})

    async def customer_delete(self, customer_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["customer_delete"],
            {"input": {"id": customer_id}},
        )
        self._check_mutation(data, "customerDelete")
        return {"deleted": True, "id": customer_id}

    async def customer_search(self, name_query: str, page_size: int = 250) -> list[dict]:
        result = await self.customer_list(page=1, page_size=page_size)
        q = name_query.lower()
        return [c for c in result["customers"] if q in (c.get("name") or "").lower()]

    # ── Vendor operations (read-only) ────────────────────────────────

    async def vendor_list(self, page: int = 1, page_size: int = 50) -> dict:
        data = await self._call_with_backoff(
            QUERIES["vendor_list"],
            {"businessId": self.business_id, "page": page, "pageSize": page_size},
        )
        vendors_data = data["business"]["vendors"]
        return {
            "pageInfo": vendors_data["pageInfo"],
            "vendors": self._extract_nodes(vendors_data["edges"]),
        }

    async def vendor_get(self, vendor_id: str) -> dict:
        data = await self._call_with_backoff(
            QUERIES["vendor_get"],
            {"businessId": self.business_id, "vendorId": vendor_id},
        )
        return data["business"]["vendor"]

    async def vendor_search(self, name_query: str, page_size: int = 250) -> list[dict]:
        result = await self.vendor_list(page=1, page_size=page_size)
        q = name_query.lower()
        return [v for v in result["vendors"] if q in (v.get("name") or "").lower()]

    # ── Invoice operations ───────────────────────────────────────────

    async def invoice_list(self, page: int = 1, page_size: int = 50) -> dict:
        data = await self._call_with_backoff(
            QUERIES["invoice_list"],
            {"businessId": self.business_id, "page": page, "pageSize": page_size},
        )
        invoices_data = data["business"]["invoices"]
        return {
            "pageInfo": invoices_data["pageInfo"],
            "invoices": self._extract_nodes(invoices_data["edges"]),
        }

    async def invoice_get(self, invoice_id: str) -> dict:
        data = await self._call_with_backoff(
            QUERIES["invoice_get"],
            {"businessId": self.business_id, "invoiceId": invoice_id},
        )
        return data["business"]["invoice"]

    async def invoice_create(self, input_data: dict) -> dict:
        input_data["businessId"] = self.business_id
        data = await self._call_with_backoff(MUTATIONS["invoice_create"], {"input": input_data})
        result = self._check_mutation(data, "invoiceCreate")
        return result.get("invoice", {})

    async def invoice_update(self, invoice_id: str, input_data: dict) -> dict:
        input_data["id"] = invoice_id
        data = await self._call_with_backoff(MUTATIONS["invoice_patch"], {"input": input_data})
        result = self._check_mutation(data, "invoicePatch")
        return result.get("invoice", {})

    async def invoice_delete(self, invoice_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["invoice_delete"],
            {"input": {"invoiceId": invoice_id}},
        )
        self._check_mutation(data, "invoiceDelete")
        return {"deleted": True, "id": invoice_id}

    async def invoice_clone(self, invoice_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["invoice_clone"],
            {"input": {"invoiceId": invoice_id}},
        )
        result = self._check_mutation(data, "invoiceClone")
        return result.get("invoice", {})

    async def invoice_send(self, invoice_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["invoice_send"],
            {"input": {"invoiceId": invoice_id}},
        )
        self._check_mutation(data, "invoiceSend")
        return {"sent": True, "id": invoice_id}

    async def invoice_approve(self, invoice_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["invoice_approve"],
            {"input": {"invoiceId": invoice_id}},
        )
        result = self._check_mutation(data, "invoiceApprove")
        return result.get("invoice", {})

    async def invoice_mark_sent(self, invoice_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["invoice_mark_sent"],
            {"input": {"invoiceId": invoice_id}},
        )
        result = self._check_mutation(data, "invoiceMarkSent")
        return result.get("invoice", {})

    async def invoice_search(self, query: str, page_size: int = 250) -> list[dict]:
        result = await self.invoice_list(page=1, page_size=page_size)
        q = query.lower()
        return [
            inv for inv in result["invoices"]
            if q in (inv.get("invoiceNumber") or "").lower()
            or q in (inv.get("customer", {}).get("name") or "").lower()
            or q in (inv.get("memo") or "").lower()
        ]

    # ── Invoice payment ──────────────────────────────────────────────

    async def invoice_payment_create(self, input_data: dict) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["invoice_payment_create_manual"],
            {"input": input_data},
        )
        result = self._check_mutation(data, "invoicePaymentCreateManual")
        return result.get("invoicePayment", {})

    # ── Money transaction (create-only, BETA) ────────────────────────

    async def money_transaction_create(self, input_data: dict) -> dict:
        input_data["businessId"] = self.business_id
        data = await self._call_with_backoff(
            MUTATIONS["money_transaction_create"],
            {"input": input_data},
        )
        result = self._check_mutation(data, "moneyTransactionCreate")
        return result.get("transaction", {})

    async def money_transactions_create(self, input_data: dict) -> dict:
        input_data["businessId"] = self.business_id
        data = await self._call_with_backoff(
            MUTATIONS["money_transactions_create"],
            {"input": input_data},
        )
        result = self._check_mutation(data, "moneyTransactionsCreate")
        return result.get("transactions", [])

    # ── Product operations ───────────────────────────────────────────

    async def product_list(self, page: int = 1, page_size: int = 50) -> dict:
        data = await self._call_with_backoff(
            QUERIES["product_list"],
            {"businessId": self.business_id, "page": page, "pageSize": page_size},
        )
        products_data = data["business"]["products"]
        return {
            "pageInfo": products_data["pageInfo"],
            "products": self._extract_nodes(products_data["edges"]),
        }

    async def product_get(self, product_id: str) -> dict:
        data = await self._call_with_backoff(
            QUERIES["product_get"],
            {"businessId": self.business_id, "productId": product_id},
        )
        return data["business"]["product"]

    async def product_create(self, input_data: dict) -> dict:
        input_data["businessId"] = self.business_id
        data = await self._call_with_backoff(MUTATIONS["product_create"], {"input": input_data})
        result = self._check_mutation(data, "productCreate")
        return result.get("product", {})

    async def product_update(self, product_id: str, input_data: dict) -> dict:
        input_data["id"] = product_id
        data = await self._call_with_backoff(MUTATIONS["product_patch"], {"input": input_data})
        result = self._check_mutation(data, "productPatch")
        return result.get("product", {})

    async def product_archive(self, product_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["product_archive"],
            {"input": {"id": product_id}},
        )
        result = self._check_mutation(data, "productArchive")
        return result.get("product", {})

    async def product_search(self, name_query: str, page_size: int = 250) -> list[dict]:
        result = await self.product_list(page=1, page_size=page_size)
        q = name_query.lower()
        return [p for p in result["products"] if q in (p.get("name") or "").lower()]

    # ── Sales tax operations ─────────────────────────────────────────

    async def sales_tax_list(self) -> list[dict]:
        data = await self._call_with_backoff(
            QUERIES["sales_tax_list"],
            {"businessId": self.business_id},
        )
        return self._extract_nodes(data["business"]["salesTaxes"]["edges"])

    async def sales_tax_get(self, tax_id: str) -> dict:
        data = await self._call_with_backoff(
            QUERIES["sales_tax_get"],
            {"businessId": self.business_id, "salesTaxId": tax_id},
        )
        return data["business"]["salesTax"]

    async def sales_tax_create(self, input_data: dict) -> dict:
        input_data["businessId"] = self.business_id
        data = await self._call_with_backoff(MUTATIONS["sales_tax_create"], {"input": input_data})
        result = self._check_mutation(data, "salesTaxCreate")
        return result.get("salesTax", {})

    async def sales_tax_update(self, tax_id: str, input_data: dict) -> dict:
        input_data["id"] = tax_id
        data = await self._call_with_backoff(MUTATIONS["sales_tax_patch"], {"input": input_data})
        result = self._check_mutation(data, "salesTaxPatch")
        return result.get("salesTax", {})

    async def sales_tax_archive(self, tax_id: str) -> dict:
        data = await self._call_with_backoff(
            MUTATIONS["sales_tax_archive"],
            {"input": {"id": tax_id}},
        )
        result = self._check_mutation(data, "salesTaxArchive")
        return result.get("salesTax", {})

    # ── Estimate operations (read-only) ──────────────────────────────

    async def estimate_list(self, page: int = 1, page_size: int = 50) -> dict:
        data = await self._call_with_backoff(
            QUERIES["estimate_list"],
            {"businessId": self.business_id, "page": page, "pageSize": page_size},
        )
        estimates_data = data["business"]["estimates"]
        return {
            "pageInfo": estimates_data["pageInfo"],
            "estimates": self._extract_nodes(estimates_data["edges"]),
        }

    async def estimate_get(self, estimate_id: str) -> dict:
        data = await self._call_with_backoff(
            QUERIES["estimate_get"],
            {"businessId": self.business_id, "estimateId": estimate_id},
        )
        return data["business"]["estimate"]

    # ── Reference operations ─────────────────────────────────────────

    async def businesses_list(self) -> list[dict]:
        data = await self._call_with_backoff(QUERIES["businesses_list"])
        return self._extract_nodes(data["businesses"]["edges"])

    async def business_get(self) -> dict:
        data = await self._call_with_backoff(
            QUERIES["business_get"],
            {"businessId": self.business_id},
        )
        return data["business"]

    async def user_get(self) -> dict:
        data = await self._call_with_backoff(QUERIES["user_get"])
        return data["user"]

    async def currencies_list(self) -> list[dict]:
        data = await self._call_with_backoff(QUERIES["currencies_list"])
        return data["currencies"]

    async def countries_list(self) -> list[dict]:
        data = await self._call_with_backoff(QUERIES["countries_list"])
        return data["countries"]

    async def account_types_list(self) -> list[dict]:
        data = await self._call_with_backoff(QUERIES["account_types_list"])
        return data["accountTypes"]

    async def account_subtypes_list(self) -> list[dict]:
        data = await self._call_with_backoff(QUERIES["account_subtypes_list"])
        return data["accountSubtypes"]

    # ── Report synthesis ─────────────────────────────────────────────

    async def report_business_summary(self) -> dict:
        """Synthesize a business summary from business info + account balances."""
        business = await self.business_get()
        accounts_result = await self.account_list(page=1, page_size=250)
        accounts = accounts_result["accounts"]

        # Group accounts by type and sum balances
        by_type = {}
        for acct in accounts:
            type_name = acct.get("type", {}).get("name", "Unknown")
            if type_name not in by_type:
                by_type[type_name] = {"accounts": [], "count": 0}
            by_type[type_name]["accounts"].append({
                "name": acct.get("name"),
                "subtype": acct.get("subtype", {}).get("name"),
                "isArchived": acct.get("isArchived"),
            })
            by_type[type_name]["count"] += 1

        return {
            "business": business,
            "accountSummary": by_type,
            "totalAccounts": len(accounts),
        }

    async def report_account_balances(self) -> list[dict]:
        """List all accounts with their type and balance info."""
        result = await self.account_list(page=1, page_size=250)
        return [
            {
                "id": a.get("id"),
                "name": a.get("name"),
                "type": a.get("type", {}).get("name"),
                "subtype": a.get("subtype", {}).get("name"),
                "normalBalanceType": a.get("normalBalanceType"),
                "balance": a.get("balance"),
                "balanceInBusinessCurrency": a.get("balanceInBusinessCurrency"),
                "isArchived": a.get("isArchived"),
            }
            for a in result["accounts"]
        ]

    async def report_customer_balances(self) -> list[dict]:
        """List all customers with outstanding/overdue amounts."""
        result = await self.customer_list(page=1, page_size=250)
        return [
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "outstandingAmount": c.get("outstandingAmount"),
                "overdueAmount": c.get("overdueAmount"),
            }
            for c in result["customers"]
        ]
