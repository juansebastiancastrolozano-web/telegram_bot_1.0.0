import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def supabase_select(table: str, filters: dict = None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {}

    if filters:
        for key, value in filters.items():
            params[f"{key}.eq"] = value

    r = requests.get(url, headers=HEADERS, params=params)

    if r.status_code != 200:
        return None

    return r.json()

def supabase_insert(table: str, data: dict):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, headers=HEADERS, json=data)

    if r.status_code not in (200, 201):
        return None

    return r.json()

