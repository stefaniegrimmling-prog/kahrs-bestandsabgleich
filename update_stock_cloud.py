#!/usr/bin/env python3
"""
Kahrs → Shopify Bestandsabgleich (Cloud-Version für GitHub Actions)
Liest Konfiguration aus Environment-Variablen statt .env-Datei.
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime
from io import StringIO

# Konfiguration aus Environment-Variablen
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
KAHRS_CSV_URL = os.environ.get("KAHRS_CSV_URL", "https://holz-kahrs.de/media/export_data/holz_kahrs-983c3908.csv")

# Produkte: Basis-SKUs der ausgewählten Produkte
SELECTED_PRODUCTS = {
    # --- Bangkirai ---
    "00002953",    # Bangkirai 25x145 KD grob/fein PREMIUM
    "18-200144",   # Bangkirai 25x145 KD grob/fein PREMIUM
    "18-205124",   # Bangkirai 25x145 KD glatt PREMIUM
    "18-205125",   # Bangkirai 45x145 KD glatt/grob PREMIUM
    "18-205142",   # Bangkirai 25x145 KD grob/fein Standard
    "18-205143",   # Bangkirai 25x145 KD glatt/glatt Standard
    # --- Bongossi ---
    "00003022",    # Bongossi 35x140 AD grob
    "00003023",    # Bongossi 35x140 AD glatt
    "00003027",    # Bongossi 40x140 AD glatt
    "00003030",    # Bongossi 45x140 AD grob
    "00022686",    # Bongossi 65x140 AD grob
    "00022706",    # Bongossi 40x140 AD grob
    "00022707",    # Bongossi 45x140 AD glatt
    "18-204273",   # Bongossi 35x140 AD glatt
    "18-204551",   # Bongossi 35x140 AD grob
    "18-204552",   # Bongossi 45x140 AD glatt
    "18-204553",   # Bongossi 45x140 AD grob
    # --- Cumaru ---
    "00003049",    # Cumaru 21x145 KD glatt/glatt
    "00003049-K",  # Cumaru 21x145 KD Kurzlängen
    "00017565",    # Cumaru 21x145 KD glatt/glatt
    "00020723",    # Cumaru 35x145 AD
    "00021843",    # Cumaru 25x145 KD glatt/glatt
    "00022953",    # Cumaru 25x145 KD glatt/glatt
    "18-201623",   # Cumaru 21x90 KD glatt/glatt
    "18-202976",   # Cumaru 45x145 KD glatt/glatt
    "18-202976-K", # Cumaru 45x145 KD Kurzlänge
    "18-204470",   # Cumaru 40x145 KD glatt/glatt
    # --- Eiche ---
    "18-200110",   # Eiche 23x140 KD Rustikal
    "18-201067",   # Eiche 23x140 KD Exklusiv
    # --- Garapa ---
    "00003164-B",  # Garapa 25x145 KD glatt/glatt
    "00010127",    # Garapa 21x145 KD glatt/glatt
    "00020155",    # Garapa 21x145 KD glatt/glatt
    "18-201609",   # Garapa 21x90 KD glatt/glatt
    "18-204152",   # Garapa 25x145 KD glatt/glatt
    "18-204571",   # Garapa 25x90 KD glatt/glatt
    # --- Guyana Ipe (Tanimbuca) ---
    "18-202375",   # Guyana Ipe 25x140 KD glatt/glatt
    # --- Guyana Teak (Basralocus) ---
    "00003347",    # Guyana Teak 25x140 KD glatt/glatt
    "00085092",    # Guyana Teak 25x140 KD glatt/glatt
    "00085113",    # Guyana Teak 25x90 KD glatt/glatt
    "18-202391-B", # Guyana Teak 21x90 KD glatt/glatt
    # --- Ipe ---
    "00003208-B",  # Ipe 21x145 KD glatt/glatt
    "00017472",    # Ipe 21x145 AD glatt/glatt
    "00021820",    # Ipe 21x145 KD glatt/glatt
    "00064008",    # Ipe 25x140 AD glatt/glatt
    "18-202533",   # Ipe 19x140 KD Bolivien
    "18-202534",   # Ipe 19x85 KD Bolivien
    "18-202539",   # Ipe 21x120 AD Bolivien
    "18-202540",   # Ipe 21x145 AD Bolivien
    "18-204088",   # Ipe 25x145 KD glatt/glatt
    "18-204406",   # Ipe 19x140 KD glatt/glatt
    "18-204535",   # Ipe 25x140 KD glatt/glatt
    "18-204628",   # Ipe 21x143 AD glatt/glatt
    # --- Sonstige ---
    "18-204152",   # Garapa 25x145 KD glatt/glatt
}

LOG_FILE = "stock_update.log"


def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def shopify_api(endpoint, method="GET", data=None, retries=5):
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    body = json.dumps(data).encode("utf-8") if data else None
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                time.sleep(0.6)  # max ~1.6 calls/sec, unter dem 2/sec Limit
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            if e.code == 429:
                wait = 2 ** attempt  # exponential backoff: 1, 2, 4, 8, 16 sec
                log(f"Rate limit (429) – warte {wait}s (Versuch {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            log(f"API Fehler {e.code}: {error_body[:200]}")
            return None
    log(f"API Fehler: Maximale Versuche erreicht für {endpoint}")
    return None


def get_base_sku(nummer):
    return nummer.split(".")[0]


def download_and_parse_kahrs():
    """Lädt Kahrs-CSV herunter und gibt {SKU: bestand} zurück."""
    log("Lade Kahrs-CSV herunter...")
    local_file = "kahrs_source.csv"
    urllib.request.urlretrieve(KAHRS_CSV_URL, local_file)
    log("Download abgeschlossen.")

    stock = {}
    with open(local_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";", quotechar='"')
        for row in reader:
            nummer = row["Nummer"]
            base = get_base_sku(nummer)
            if base in SELECTED_PRODUCTS:
                # Überspringe Musterplatten und Pakete
                if "-MP" in nummer or ".PK-" in nummer:
                    continue
                bestand = int(row.get("Lagerbestand", "0") or "0")
                stock[nummer] = bestand
    return stock


def get_shopify_products():
    sku_map = {}
    endpoint = "products.json?limit=250"
    result = shopify_api(endpoint)
    if not result:
        return sku_map

    for product in result.get("products", []):
        for variant in product.get("variants", []):
            sku = variant.get("sku", "")
            inv_item_id = variant.get("inventory_item_id")
            if sku and inv_item_id:
                sku_map[sku] = {
                    "inventory_item_id": inv_item_id,
                    "product_title": product["title"],
                    "variant_title": variant.get("title", ""),
                }
    return sku_map


def get_location_id():
    result = shopify_api("locations.json")
    if result and result.get("locations"):
        return result["locations"][0]["id"]
    return None


def update_inventory(inventory_item_id, location_id, new_qty):
    data = {
        "location_id": location_id,
        "inventory_item_id": inventory_item_id,
        "available": new_qty,
    }
    result = shopify_api("inventory_levels/set.json", method="POST", data=data)
    return result is not None


def main():
    if not SHOPIFY_STORE or not SHOPIFY_ACCESS_TOKEN:
        log("FEHLER: SHOPIFY_STORE und SHOPIFY_ACCESS_TOKEN müssen gesetzt sein!")
        sys.exit(1)

    log("=== Bestandsabgleich gestartet ===")

    # 1. Kahrs-Bestände laden
    kahrs_stock = download_and_parse_kahrs()
    log(f"Kahrs: {len(kahrs_stock)} relevante Varianten gefunden")

    # 2. Shopify-Produkte laden
    log("Lade Shopify-Produkte...")
    shopify_products = get_shopify_products()
    log(f"Shopify: {len(shopify_products)} Varianten mit SKU gefunden")

    # 3. Location ID
    location_id = get_location_id()
    if not location_id:
        log("FEHLER: Keine Shopify-Location gefunden!")
        sys.exit(1)

    # 4. Bestände abgleichen
    updated = 0
    not_found = 0
    errors = 0

    for sku, kahrs_qty in sorted(kahrs_stock.items()):
        if sku not in shopify_products:
            log(f"  WARNUNG: SKU {sku} nicht in Shopify gefunden")
            not_found += 1
            continue

        info = shopify_products[sku]
        inv_id = info["inventory_item_id"]
        log(f"  UPDATE: {info['product_title']} [{info['variant_title']}] → {kahrs_qty} Stück")

        if update_inventory(inv_id, location_id, kahrs_qty):
            updated += 1
        else:
            log(f"  FEHLER beim Update von {sku}")
            errors += 1

    log(f"\n=== Ergebnis ===")
    log(f"  Aktualisiert: {updated}")
    log(f"  Nicht gefunden: {not_found}")
    log(f"  Fehler: {errors}")
    log(f"=== Bestandsabgleich beendet ===\n")

    if errors > updated and updated == 0:
        log("FEHLER: Kein einziges Update erfolgreich!")
        sys.exit(1)


if __name__ == "__main__":
    main()
