#!/usr/bin/env python3
"""Cloud-Preis-Waechter (GitHub Actions) -- laeuft unabhaengig vom Mac, auch am
Wochenende. Prueft JEDE aktive Variante gegen den EK und repariert echte
Unter-EK-Preise automatisch auf +7% Mindestmarge. Mailt eine Zusammenfassung,
wenn etwas gefunden/geaendert wurde (sonst still).

Gleiche geprueften Regeln wie der lokale Waechter (Stand 2026-08-03):
  - EK = preisH (Kahrs-CSV), pro Basiseinheit; Kaufeinheit!=1 -> pro Stueck.
  - Faktor aus CSV-Laenge (autoritativ); Suffix nur Fallback, keine absurden Werte.
  - Auto-Fix NUR hochsicher: lfm/m², normale Kaufeinheit, verlaessliche Masse.
  - NIE anfassen: SUSPECT/Whitelist-SKUs, Tags manual-keep/muster/preis-manuell.
  - Hebt nie ueber Notmarge, senkt nie, faellt Gesundes nie an.

Env (GitHub Secrets): SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, KAHRS_CSV_URL
  Optional fuer Mail: SMTP_USER, SMTP_PASSWORD, REPORT_TO
"""
import csv, json, os, sys, time, ssl, smtplib, urllib.request, urllib.error
from datetime import datetime, date
from email.mime.text import MIMEText

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
KAHRS_CSV = os.path.join(SCRIPT_DIR, 'kahrs_source.csv')
KAHRS_URL = os.environ.get('KAHRS_CSV_URL', 'https://holz-kahrs.de/media/export_data/holz_kahrs-983c3908.csv')

VAT = 1.19
MIN_MARGIN = 1.07          # +7% Mindestmarge beim Auto-Fix
DUENN_MARGIN = 1.05
# Whitelist: geprueft ok / bekannte Ausnahmen -> ganz ueberspringen (kein Alarm).
WHITELIST = {'00021382', '00021384', '00021368'}
# Stems mit unzuverlaessiger Einheit -> nie AUTO-fixen, nur melden (falls je Unter-EK).
NO_TOUCH = {'00020553-DIM'}
PROTECT_TAGS = {'manual-keep', 'muster', 'preis-manuell'}


def log(m):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {m}", flush=True)


def load_env():
    env = {}
    for k in ('SHOPIFY_STORE', 'SHOPIFY_ACCESS_TOKEN'):
        v = os.environ.get(k, '').strip()
        if not v:
            print(f"FEHLER: Env {k} fehlt"); sys.exit(1)
        env[k] = v
    for k in ('SMTP_USER', 'SMTP_PASSWORD', 'REPORT_TO', 'REPORT_FROM'):
        env[k] = os.environ.get(k, '').strip()
    return env


def api(env, endpoint, method='GET', data=None, _attempt=0):
    url = f"https://{env['SHOPIFY_STORE']}/admin/api/2024-01/{endpoint}"
    headers = {'X-Shopify-Access-Token': env['SHOPIFY_ACCESS_TOKEN'], 'Content-Type': 'application/json'}
    body = json.dumps(data).encode('utf-8') if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            link = resp.headers.get('Link', '')
            limit = resp.headers.get('X-Shopify-Shop-Api-Call-Limit', '')
            if limit:
                used, total = [int(x) for x in limit.split('/')]
                if used >= total - 2:
                    time.sleep(1.0)
            return json.loads(resp.read().decode('utf-8')), link
    except urllib.error.HTTPError as e:
        if e.code == 429 and _attempt < 5:
            time.sleep(float(e.headers.get('Retry-After', '2')) * (1 + _attempt))
            return api(env, endpoint, method, data, _attempt + 1)
        if e.code in (500, 502, 503, 504) and _attempt < 3:
            time.sleep(2 * (_attempt + 1))
            return api(env, endpoint, method, data, _attempt + 1)
        log(f"  API {e.code} {endpoint[:60]}")
        return None, ''
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        if _attempt < 5:
            time.sleep(3 * (_attempt + 1))
            return api(env, endpoint, method, data, _attempt + 1)
        log(f"  NET {endpoint[:60]}: {e}")
        return None, ''


def download_kahrs():
    for attempt in range(3):
        try:
            req = urllib.request.Request(KAHRS_URL, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
            if len(data) < 10000:
                raise ValueError(f"CSV zu klein: {len(data)}")
            with open(KAHRS_CSV, 'wb') as f:
                f.write(data)
            log(f"Kahrs-CSV geladen ({len(data)} bytes)")
            return
        except Exception as e:
            log(f"  Download-Fehler {attempt+1}/3: {e}")
            time.sleep(5 * (attempt + 1))
    raise RuntimeError("Kahrs-CSV-Download fehlgeschlagen")


def load_kahrs():
    k = {}
    with open(KAHRS_CSV, encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f, delimiter=';', quotechar='"'):
            sku = (row.get('Nummer') or '').strip()
            raw = (row.get('preisH') or '').split('|')[0]
            if not sku or ':' not in raw:
                continue
            try:
                ek = float(raw.split(':', 1)[1])
            except ValueError:
                continue

            def num(v):
                try:
                    return float((v or '0').replace(',', '.'))
                except Exception:
                    return 0.0
            be = (row.get('Basiseinheit') or '').strip().lower()
            if be == 'qm':
                be = 'm²'
            k[sku] = {'ek': ek, 'basis': be, 'laenge_mm': num(row.get('Länge')),
                      'breite_mm': num(row.get('Breite')), 'kauf': num(row.get('Kaufeinheit'))}
    return k


def faktor(sku, k):
    ke = k.get('kauf', 0.0)
    if ke and abs(ke - 1.0) > 0.01:
        return 1.0
    basis = k['basis']
    L = k['laenge_mm']
    if L <= 0 and '.' in sku and sku.split('.', 1)[1][:1].isdigit():
        suf = sku.split('.', 1)[1].split('.')[0]
        if suf.isdigit() and int(suf) > 0:
            L_suf = int(suf) / 100000.0
            if 0 < L_suf <= 20000:
                L = L_suf
    if basis == 'lfm':
        return L / 1000.0 if L > 0 else 1.0
    if basis in ('m²', 'qm'):
        if L > 0 and k['breite_mm'] > 0:
            return L * k['breite_mm'] / 1_000_000.0
        return 1.0
    return 1.0


def fetch_variants(env):
    out = {}
    ep = 'products.json?limit=250&status=active&fields=id,title,tags,variants'
    while ep:
        r, link = api(env, ep)
        if r is None:
            return None
        for p in r.get('products', []):
            tags = {t.strip() for t in (p.get('tags') or '').split(',') if t.strip()}
            for v in p.get('variants', []):
                if v.get('sku'):
                    out[v['sku']] = {'id': v['id'], 'price': float(v.get('price') or 0),
                                     'title': p['title'], 'tags': tags}
        ep = None
        if 'rel="next"' in link:
            for part in link.split(','):
                if 'rel="next"' in part:
                    ep = part.split(';')[0].strip().strip('<>').split('/admin/api/2024-01/')[-1]
        time.sleep(0.3)
    return out


def main():
    log("=== Cloud-Preis-Waechter gestartet ===")
    env = load_env()
    download_kahrs()
    kahrs = load_kahrs()
    vmap = fetch_variants(env)
    if vmap is None:
        _mail(env, "FEHLER Cloud-Preis-Waechter", "Shopify nicht erreichbar -- keine Preispruefung gelaufen!")
        sys.exit(1)
    log(f"Kahrs {len(kahrs)} SKUs, Shopify {len(vmap)} Varianten")

    fixed, alarm_only = [], []
    for sku, vi in vmap.items():
        if sku not in kahrs or sku in WHITELIST:
            continue
        k = kahrs[sku]
        ek = k['ek']
        if ek <= 0:
            continue
        f = faktor(sku, k)
        vk = vi['price']
        if vk <= 0 or f is None:
            continue
        vk_net = (vk / f) / VAT
        if vk_net >= ek - 0.001:
            continue  # nicht unter EK

        stem = sku.split('.')[0]
        protected = bool(vi['tags'] & PROTECT_TAGS)
        unit_ok = k['basis'] in ('lfm', 'm²') and not (k.get('kauf') and abs(k['kauf'] - 1.0) > 0.01)
        safe = unit_ok and stem not in NO_TOUCH and sku not in NO_TOUCH and not protected
        neu = round(ek * f * MIN_MARGIN * VAT, 2)
        rec = (sku, round(ek, 2), round(vk, 2), neu, vi['title'][:45], vi['id'])
        (fixed if safe else alarm_only).append(rec)

    changed = 0
    for sku, ek, alt, neu, name, vid in fixed:
        vid_num = str(vid).split('/')[-1]
        r, _ = api(env, f"variants/{vid_num}.json", 'PUT', {'variant': {'id': int(vid_num), 'price': f'{neu:.2f}'}})
        if r:
            changed += 1
            log(f"  AUTO-FIX {sku}: {alt} -> {neu}")
        time.sleep(0.3)

    log(f"Fertig. Auto-korrigiert: {changed}, nur Alarm: {len(alarm_only)}")
    if fixed or alarm_only:
        lines = [f"Cloud-Preis-Waechter {date.today().isoformat()} (laeuft auch bei ausgeschaltetem Mac)", ""]
        if fixed:
            lines.append(f"AUTOMATISCH KORRIGIERT ({changed}):")
            lines += [f"  {s}  {a} -> {n} EUR  {nm}" for s, ek, a, n, nm, _ in fixed]
            lines.append("")
        if alarm_only:
            lines.append(f"NUR ALARM -- manuell pruefen ({len(alarm_only)}):")
            lines += [f"  {s}  VK {a} unter EK {ek}  {nm}" for s, ek, a, n, nm, _ in alarm_only]
        _mail(env, f"Cloud-Preis-Waechter: {changed} korrigiert", "\n".join(lines))
    log("=== Cloud-Preis-Waechter beendet ===")


def _mail(env, subject, body):
    if not (env.get('SMTP_PASSWORD') and env.get('REPORT_TO')):
        log("Kein SMTP-Secret -- keine Mail (Auto-Fix lief trotzdem).")
        return
    try:
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = f"Holzagenten24 -- {subject} ({date.today().isoformat()})"
        msg['From'] = env.get('REPORT_FROM') or env['REPORT_TO']
        msg['To'] = env['REPORT_TO']
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=ssl.create_default_context()) as srv:
            srv.login(env.get('SMTP_USER') or env['REPORT_TO'], env['SMTP_PASSWORD'])
            srv.send_message(msg)
        log("Mail versendet.")
    except Exception as e:
        log(f"Mailversand fehlgeschlagen: {e}")


if __name__ == '__main__':
    main()
