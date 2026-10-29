#!/usr/bin/env python3
"""Mindestmenge + Bestellschritt je Variante aus der Kahrs-CSV (eine Stelle, seit 29.10.2026).

Setzt custom.min_order_qty / custom.qty_step (number_integer) an der Variante, genau die Namen,
die Theme und Mindestabnahme-Function lesen.

Warum ein eigenes Modul: Bestand und Mindestmenge kamen zu verschiedenen Zeiten in den Shop.
Den Bestand setzt der Bestand-Sync (nachts auch in der Cloud), die Mindestmenge nur die lokale
kahrs_uebernahme Stunden spaeter. Dazwischen war ein Artikel kaufbar, aber nicht bestellbar
(Laerche Glattkant 26.10., Fichte weiss grundiert 29.10.2026: Kahrs-min 6 -> 4, Bestand 4,
Shop noch 6, Warenkorb HTTP 422). Jetzt ziehen stock_sync.py und update_stock_cloud.py die
Mindestmenge im selben Lauf direkt nach dem Bestand mit; apply_min_order_qty.py nutzt dieselbe
Regel.

github-repo/mindestmenge.py ist eine Kopie fuer den Cloud-Lauf, beide muessen gleich bleiben
(tests/test_mindestmenge.py prueft das).

Regeln (Memory project_mindestabnahme, Stand 04.09./05.10.2026):
  - min = mindestAbnahme, step = Bestellschritt (0 oder 1 -> 1)
  - Kahrs 1/1: nur eingreifen, wenn im Shop noch ein alter Wert > 1 steht
  - sonst nur, wenn Bestand >= min (sonst ohnehin unverkaeuflich), ausser inventoryPolicy CONTINUE
  - muster / manual-keep nie anfassen; unveraenderte Werte ueberspringen
"""
import csv
import json
import time
import urllib.error
import urllib.request

API = '2024-07'
SCHUTZ_TAGS = ('muster', 'manual-keep')


def _zahl(x):
    try:
        return int(float((x or '0').replace(',', '.')))
    except ValueError:
        return 0


def soll_aus_csv(pfad):
    """SKU -> (min, step) fuer ALLE SKUs der CSV, auch 1/1 (damit alte Werte zurueck auf 1 gehen)."""
    out = {}
    with open(pfad, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f, delimiter=';'):
            sku = (r.get('Nummer') or '').strip()
            if not sku or '.PK-' in sku:
                continue
            out[sku] = (max(1, _zahl(r.get('mindestAbnahme'))), max(1, _zahl(r.get('Bestellschritt'))))
    return out


def plan(soll, varianten):
    """varianten: dicts mit id, sku, bestand, policy, tags, min, step (min/step = Shop-Wert oder None).
    Gibt (aenderungen, uebersprungen) zurueck; aenderung = (id, sku, min, step, (min_alt, step_alt))."""
    aend, skip = [], []
    for v in varianten:
        sku = (v.get('sku') or '').strip()
        if sku not in soll or any(t in v.get('tags', ()) for t in SCHUTZ_TAGS):
            continue
        mn, st = soll[sku]
        alt = (v.get('min'), v.get('step'))
        if (mn, st) == (1, 1):
            if (alt[0] or 1) > 1 or (alt[1] or 1) > 1:
                aend.append((v['id'], sku, 1, 1, alt))
            continue
        if v.get('policy') != 'CONTINUE' and (v.get('bestand') or 0) < mn:
            skip.append((sku, mn, v.get('bestand')))
            continue
        if alt != (mn, st):
            aend.append((v['id'], sku, mn, st, alt))
    return aend, skip


def _gql(env, q, v=None):
    url = f"https://{env['SHOPIFY_STORE']}/admin/api/{API}/graphql.json"
    req = urllib.request.Request(url, data=json.dumps({'query': q, 'variables': v or {}}).encode(),
                                 headers={'X-Shopify-Access-Token': env['SHOPIFY_ACCESS_TOKEN'],
                                          'Content-Type': 'application/json'})
    for i in range(4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.load(r)
            if d.get('errors') and 'THROTTLED' in json.dumps(d['errors']):
                time.sleep(3 * (i + 1))
                continue
            return d
        except (urllib.error.URLError, TimeoutError):
            time.sleep(3 * (i + 1))
    raise RuntimeError('Shopify GraphQL nicht erreichbar')


_Q = '''query($c:String){ productVariants(first:250, after:$c, query:"product_status:active"){
  pageInfo{hasNextPage endCursor}
  nodes{ id sku inventoryQuantity inventoryPolicy product{ id title tags }
    mn: metafield(namespace:"custom",key:"min_order_qty"){value}
    st: metafield(namespace:"custom",key:"qty_step"){value} } } }'''


def shop_varianten(env, nur_produkt=None):
    """Alle aktiven Varianten, voll paginiert. nur_produkt(pid) -> bool filtert (Upload-Scope)."""
    out, cur = [], None
    while True:
        d = _gql(env, _Q, {'c': cur})
        if 'data' not in d or not d['data']:
            raise RuntimeError(f"Variantenliste unvollstaendig: {str(d.get('errors'))[:200]}")
        seite = d['data']['productVariants']
        for n in seite['nodes']:
            if nur_produkt and not nur_produkt(n['product']['id']):
                continue
            out.append({'id': n['id'], 'sku': n['sku'], 'bestand': n['inventoryQuantity'],
                        'policy': n['inventoryPolicy'], 'tags': n['product']['tags'],
                        'titel': n['product']['title'],
                        'min': int(n['mn']['value']) if n['mn'] else None,
                        'step': int(n['st']['value']) if n['st'] else None})
        if not seite['pageInfo']['hasNextPage']:
            return out
        cur = seite['pageInfo']['endCursor']


_M = 'mutation($m:[MetafieldsSetInput!]!){ metafieldsSet(metafields:$m){ userErrors{field message} } }'


def setzen(env, aenderungen):
    """Schreibt min/step. Gibt (ok, fehler) zurueck."""
    ok = err = 0
    for i in range(0, len(aenderungen), 12):
        teil = aenderungen[i:i + 12]
        m = []
        for vid, _sku, mn, st, _alt in teil:
            m.append({'ownerId': vid, 'namespace': 'custom', 'key': 'min_order_qty',
                      'type': 'number_integer', 'value': str(mn)})
            m.append({'ownerId': vid, 'namespace': 'custom', 'key': 'qty_step',
                      'type': 'number_integer', 'value': str(st)})
        d = _gql(env, _M, {'m': m})
        ue = (d.get('data') or {}).get('metafieldsSet', {}).get('userErrors') or d.get('errors')
        if ue:
            err += len(teil)
        else:
            ok += len(teil)
        time.sleep(0.3)
    return ok, err


def abgleichen(env, csv_pfad, live, log=print):
    """Ein Durchgang fuer den Bestand-Sync: Soll aus CSV, Ist aus dem Shop, Abweichungen setzen.
    Wirft keine Ausnahme nach oben: der Bestand-Sync soll an der Mindestmenge nicht scheitern."""
    try:
        aend, skip = plan(soll_aus_csv(csv_pfad), shop_varianten(env))
        for vid, sku, mn, st, alt in aend[:20]:
            log(f"  MIN {sku:26} min={mn} step={st} (war {alt[0]}/{alt[1]})")
        if live and aend:
            ok, err = setzen(env, aend)
        else:
            ok, err = 0, 0
        log(f"  Mindestmenge: {len(aend)} abweichend, gesetzt {ok}, Fehler {err}"
            f"{'' if live else ' (Trockenlauf)'}, Bestand < Mindestmenge {len(skip)}")
        return len(aend), ok, err
    except Exception as e:
        log(f"  ! Mindestmenge nicht abgeglichen: {e}")
        return 0, 0, 1
