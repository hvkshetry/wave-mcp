#!/usr/bin/env python3
"""Wave business ID discovery — reads token from .env, lists businesses, writes WAVE_BUSINESS_ID."""

import os
import re
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

GRAPHQL_URL = "https://gql.waveapps.com/graphql/public"

BUSINESSES_QUERY = """
query {
  businesses {
    edges {
      node {
        id
        name
      }
    }
  }
}
"""


def discover_business_id():
    token = os.getenv("WAVE_ACCESS_TOKEN")
    if not token:
        print("ERROR: WAVE_ACCESS_TOKEN not set in .env")
        print("Get your Full Access token from Wave > Settings > API Applications")
        sys.exit(1)

    print(f"Using token: {token[:6]}...{token[-4:]}")

    resp = httpx.post(
        GRAPHQL_URL,
        json={"query": BUSINESSES_QUERY},
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        print(f"ERROR: GraphQL errors: {data['errors']}")
        sys.exit(1)

    edges = data["data"]["businesses"]["edges"]
    if not edges:
        print("ERROR: No businesses found for this token.")
        sys.exit(1)

    businesses = [edge["node"] for edge in edges]

    if len(businesses) == 1:
        biz = businesses[0]
        print(f"Found 1 business: {biz['name']} (ID: {biz['id']})")
        selected = biz
    else:
        print(f"Found {len(businesses)} businesses:")
        for i, biz in enumerate(businesses, 1):
            print(f"  {i}. {biz['name']} (ID: {biz['id']})")
        while True:
            choice = input(f"Select business [1-{len(businesses)}]: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(businesses):
                selected = businesses[int(choice) - 1]
                break
            print("Invalid choice, try again.")

    # Write to .env
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    try:
        with open(env_path, "r") as f:
            content = f.read()
    except FileNotFoundError:
        content = f"WAVE_ACCESS_TOKEN={token}\n"

    if re.search(r"^WAVE_BUSINESS_ID=", content, re.MULTILINE):
        content = re.sub(
            r"^WAVE_BUSINESS_ID=.*$",
            f"WAVE_BUSINESS_ID={selected['id']}",
            content,
            flags=re.MULTILINE,
        )
    else:
        content += f"\nWAVE_BUSINESS_ID={selected['id']}\n"

    with open(env_path, "w") as f:
        f.write(content)

    print(f"\nWrote WAVE_BUSINESS_ID={selected['id']} to .env")
    print("You can now run: uv run python server.py")


if __name__ == "__main__":
    discover_business_id()
