#!/usr/bin/env python3
"""
daily_report.py — Täglicher Morgens-Report für Holzagenten24.

Pulls täglich GA4 + Search Console + Shopify Admin API + Health-Check,
erkennt Anomalien und schickt priorisierte To-Dos als HTML-Mail.

Lauf lokal:  python3 daily_report.py [--dry-run] [--date 2026-05-26]
Cloud:       GitHub Actions täglich 09:00 MESZ (siehe daily-report.yml)

Voraussetzungen (siehe SETUP_DAILY_REPORT.md):
  - .gcp/ga4-sa.json  (Service-Account mit Zugriff auf GA4 + GSC)
  - .env:
      SHOPIFY_STORE=...        (existiert bereits)
      SHOPIFY_ACCESS_TOKEN=... (existiert bereits)
      GA4_PROPERTY_ID=...
      GSC_SITE_URL=https://www.holzagenten24.de/
      REPORT_TO=stefanie.grimmling@gmail.com
      SMTP_USER=stefanie.grimmling@gmail.com
      SMTP_PASSWORD=app-password-16-stellen
"""
import os, sys, json, html, argparse, smtplib, ssl, time
from datetime import datetime, timedelta, date, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib import request as urlreq, error as urlerr

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, OrderBy
)
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Konstanten ───────────────────────────────────────────────────────────────
# Bestellungen vor diesem Datum sind Test-Käufe von Stefanie und werden ignoriert.
CONVERSION_CUTOFF = date(2026, 5, 12)
SHOP_URL = 'https://www.holzagenten24.de'
LOW_STOCK_THRESHOLD = 3
HEALTH_URLS = [
    ('Startseite', f'{SHOP_URL}/'),
    ('Sitemap', f'{SHOP_URL}/sitemap.xml'),
    ('robots.txt', f'{SHOP_URL}/robots.txt'),
    ('Beispiel-Collection', f'{SHOP_URL}/collections/terrassendielen'),
    ('Kontakt-Seite', f'{SHOP_URL}/pages/kontakt'),
]

# ── Env / Config ─────────────────────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(HERE, '.env')
if os.path.exists(ENV_PATH):
    for line in open(ENV_PATH):
        if '=' in line and not line.startswith('#'):
            k, v = line.strip().split('=', 1)
            os.environ.setdefault(k, v.strip().strip('"'))

SA_PATH = os.environ.get('GCP_SA_PATH') or os.path.join(HERE, '.gcp', 'ga4-sa.json')
PROPERTY_ID = os.environ.get('GA4_PROPERTY_ID', '').strip()
GSC_SITE = os.environ.get('GSC_SITE_URL', '').strip()
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '').strip()
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '').strip()
REPORT_TO = os.environ.get('REPORT_TO', 'stefanie.grimmling@gmail.com').strip()
REPORT_FROM = os.environ.get('REPORT_FROM', REPORT_TO).strip()
SMTP_USER = os.environ.get('SMTP_USER', '').strip()
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '').strip()
SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.gmail.com').strip()
SMTP_PORT = int(os.environ.get('SMTP_PORT', '465'))

ap = argparse.ArgumentParser()
ap.add_argument('--dry-run', action='store_true', help='HTML lokal speichern, keine Mail')
ap.add_argument('--date', help='Reporting-Tag (YYYY-MM-DD), default: gestern minus 1 (D-2)')
args = ap.parse_args()

# GA4 hat ~24-48h Latenz; D-2 ist meistens vollständig.
if args.date:
    REPORT_DATE = datetime.strptime(args.date, '%Y-%m-%d').date()
else:
    REPORT_DATE = date.today() - timedelta(days=2)

PREV_DATE = REPORT_DATE - timedelta(days=1)
SAMEDAY_LASTWEEK = REPORT_DATE - timedelta(days=7)  # gleicher Wochentag, letzte Woche
WEEK_START = REPORT_DATE - timedelta(days=7)
WEEK_END = REPORT_DATE - timedelta(days=1)
PREV_WEEK_START = REPORT_DATE - timedelta(days=14)
PREV_WEEK_END = REPORT_DATE - timedelta(days=8)
TREND_START = REPORT_DATE - timedelta(days=27)  # 28-Tage-Trend

def need(name, ok):
    if not ok:
        print(f'❌ {name} fehlt — siehe SETUP_DAILY_REPORT.md')
        sys.exit(1)

need('Service-Account-JSON', os.path.exists(SA_PATH))
need('GA4_PROPERTY_ID', PROPERTY_ID)
need('GSC_SITE_URL', GSC_SITE)
need('SHOPIFY_STORE', SHOPIFY_STORE)
need('SHOPIFY_ACCESS_TOKEN', SHOPIFY_TOKEN)
if not args.dry_run:
    need('SMTP_USER', SMTP_USER)
    need('SMTP_PASSWORD', SMTP_PASSWORD)

print(f'Report-Tag: {REPORT_DATE}')

SCOPES = [
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/webmasters.readonly',
]
creds = service_account.Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
ga = BetaAnalyticsDataClient(credentials=creds)
gsc = build('searchconsole', 'v1', credentials=creds, cache_discovery=False)

# ── GA4 Helpers ──────────────────────────────────────────────────────────────
def ga_run(dims, mets, start, end, order=None, limit=50):
    req = RunReportRequest(
        property=f'properties/{PROPERTY_ID}',
        dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in mets],
        date_ranges=[DateRange(start_date=str(start), end_date=str(end))],
        limit=limit,
    )
    if order:
        req.order_bys = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name=order), desc=True)]
    resp = ga.run_report(req)
    out = []
    for row in resp.rows:
        d = {}
        for i, dh in enumerate(resp.dimension_headers):
            d[dh.name] = row.dimension_values[i].value
        for i, mh in enumerate(resp.metric_headers):
            d[mh.name] = row.metric_values[i].value
        out.append(d)
    return out

# ── 1. GA4 ───────────────────────────────────────────────────────────────────
print('GA4: Totals + Trends…')
METRICS = ['sessions', 'totalUsers', 'engagementRate', 'bounceRate',
           'eventCount', 'screenPageViews']

def totals_for(start, end):
    r = ga_run([], METRICS, start, end)
    return r[0] if r else {}

today_t = totals_for(REPORT_DATE, REPORT_DATE)
yest_t = totals_for(PREV_DATE, PREV_DATE)
samedaylw_t = totals_for(SAMEDAY_LASTWEEK, SAMEDAY_LASTWEEK)
week_raw = totals_for(WEEK_START, WEEK_END)
prev_week_raw = totals_for(PREV_WEEK_START, PREV_WEEK_END)

def avg_dict(raw, days):
    out = {}
    for k, v in raw.items():
        try:
            fv = float(v)
            out[k] = fv if k in ('engagementRate', 'bounceRate') else fv / days
        except Exception:
            out[k] = v
    return out

week_avg = avg_dict(week_raw, 7)
prev_week_avg = avg_dict(prev_week_raw, 7)

print('GA4: Funnel + Conversion-Rates…')
ev_today = {r['eventName']: int(r['eventCount']) for r in ga_run(['eventName'], ['eventCount'], REPORT_DATE, REPORT_DATE, limit=200)}
ev_yest = {r['eventName']: int(r['eventCount']) for r in ga_run(['eventName'], ['eventCount'], PREV_DATE, PREV_DATE, limit=200)}
ev_samedaylw = {r['eventName']: int(r['eventCount']) for r in ga_run(['eventName'], ['eventCount'], SAMEDAY_LASTWEEK, SAMEDAY_LASTWEEK, limit=200)}
ev_week_total = {r['eventName']: int(r['eventCount']) for r in ga_run(['eventName'], ['eventCount'], WEEK_START, WEEK_END, limit=200)}
ev_prev_week_total = {r['eventName']: int(r['eventCount']) for r in ga_run(['eventName'], ['eventCount'], PREV_WEEK_START, PREV_WEEK_END, limit=200)}
ev_week_avg = {k: v / 7 for k, v in ev_week_total.items()}

FUNNEL = ['session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase']
FUNNEL_LABELS = {
    'session_start': 'Session-Start',
    'view_item': 'PDP-View',
    'add_to_cart': 'Warenkorb',
    'begin_checkout': 'Checkout-Start',
    'purchase': 'Kauf',
}

print('GA4: 28-Tage-Trend…')
daily_trend = ga_run(['date'], ['sessions', 'eventCount'], TREND_START, REPORT_DATE, limit=40)
daily_trend.sort(key=lambda r: r['date'])
# Käufe pro Tag aus Event-Filter holen — separater Pull weil EventName-Filter teuer
purchases_daily_resp = ga_run(['date', 'eventName'], ['eventCount'], TREND_START, REPORT_DATE, limit=500)
purchases_by_day = {}
a2c_by_day = {}
view_by_day = {}
for r in purchases_daily_resp:
    if r['eventName'] == 'purchase':
        purchases_by_day[r['date']] = int(r['eventCount'])
    elif r['eventName'] == 'add_to_cart':
        a2c_by_day[r['date']] = int(r['eventCount'])
    elif r['eventName'] == 'view_item':
        view_by_day[r['date']] = int(r['eventCount'])

print('GA4: Channels + Pages…')
lp = ga_run(['landingPage'], ['sessions', 'bounceRate', 'engagementRate'], REPORT_DATE, REPORT_DATE, order='sessions', limit=20)
ch = ga_run(['sessionDefaultChannelGroup'], ['sessions', 'engagementRate', 'conversions'], WEEK_START, WEEK_END, order='sessions', limit=10)
# Channel-Conversions: Käufe pro Channel
ch_conv = ga_run(['sessionDefaultChannelGroup', 'eventName'], ['eventCount'], WEEK_START, WEEK_END, limit=200)
ch_purchases = {}
ch_a2c = {}
ch_checkout_ch = {}
for r in ch_conv:
    if r['eventName'] == 'purchase':
        ch_purchases[r['sessionDefaultChannelGroup']] = int(r['eventCount'])
    elif r['eventName'] == 'add_to_cart':
        ch_a2c[r['sessionDefaultChannelGroup']] = int(r['eventCount'])
    elif r['eventName'] == 'begin_checkout':
        ch_checkout_ch[r['sessionDefaultChannelGroup']] = int(r['eventCount'])

paid_search = next((c for c in ch if c['sessionDefaultChannelGroup'] == 'Paid Search'), None)

# Top-Pages mit Engagement-Problem (Top 10 nach Sessions, gefiltert nach hoher Bounce)
top_pages = ga_run(['pagePath'], ['screenPageViews', 'bounceRate', 'averageSessionDuration'],
                   WEEK_START, WEEK_END, order='screenPageViews', limit=15)

# ── 2. Search Console ────────────────────────────────────────────────────────
print('GSC: Performance…')
gsc_end = REPORT_DATE - timedelta(days=1)
gsc_start = REPORT_DATE - timedelta(days=8)

def gsc_query(dims, start, end, row_limit=25):
    body = {'startDate': str(start), 'endDate': str(end), 'dimensions': dims, 'rowLimit': row_limit}
    try:
        return gsc.searchanalytics().query(siteUrl=GSC_SITE, body=body).execute().get('rows', [])
    except Exception as e:
        print(f'  ⚠ GSC-Fehler: {e}')
        return []

gsc_top_queries = gsc_query(['query'], gsc_start, gsc_end, row_limit=20)
gsc_top_pages = gsc_query(['page'], gsc_start, gsc_end, row_limit=10)

# Periodenvergleich: vor-Periode (8-15 Tage zurück)
gsc_prev_end = REPORT_DATE - timedelta(days=8)
gsc_prev_start = REPORT_DATE - timedelta(days=15)
gsc_prev_queries = {r['keys'][0]: r for r in gsc_query(['query'], gsc_prev_start, gsc_prev_end, row_limit=50)}
gsc_curr_queries = {r['keys'][0]: r for r in gsc_top_queries}

# Veränderungen berechnen
query_changes = []
all_q = set(gsc_curr_queries) | set(gsc_prev_queries)
for q in all_q:
    curr = gsc_curr_queries.get(q, {'clicks':0, 'impressions':0, 'position':0})
    prev = gsc_prev_queries.get(q, {'clicks':0, 'impressions':0, 'position':0})
    click_delta = curr.get('clicks', 0) - prev.get('clicks', 0)
    impr_delta = curr.get('impressions', 0) - prev.get('impressions', 0)
    pos_delta = (prev.get('position', 0) - curr.get('position', 0)) if (curr.get('position') and prev.get('position')) else 0
    query_changes.append({
        'query': q,
        'clicks_now': curr.get('clicks', 0),
        'clicks_prev': prev.get('clicks', 0),
        'click_delta': click_delta,
        'impr_now': curr.get('impressions', 0),
        'impr_delta': impr_delta,
        'pos_now': curr.get('position', 0),
        'pos_delta': pos_delta,
    })

gsc_winners = sorted([q for q in query_changes if q['click_delta'] > 0], key=lambda x: -x['click_delta'])[:5]
gsc_losers = sorted([q for q in query_changes if q['click_delta'] < 0], key=lambda x: x['click_delta'])[:5]
gsc_opportunities = sorted(
    [q for q in query_changes if q['impr_now'] >= 20 and q['clicks_now'] / max(q['impr_now'], 1) < 0.02],
    key=lambda x: -x['impr_now'])[:5]

# ── 3. Shopify Admin API ─────────────────────────────────────────────────────
print('Shopify: Bestellungen + Lager + Checkouts…')
SHOP_API = f'https://{SHOPIFY_STORE}/admin/api/2024-04'

def shopify_get(path, params=None):
    url = f'{SHOP_API}/{path}'
    if params:
        from urllib.parse import urlencode
        url += '?' + urlencode(params)
    req = urlreq.Request(url, headers={
        'X-Shopify-Access-Token': SHOPIFY_TOKEN,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    })
    try:
        with urlreq.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urlerr.HTTPError as e:
        print(f'  ⚠ Shopify {path}: HTTP {e.code}')
        return None
    except Exception as e:
        print(f'  ⚠ Shopify {path}: {e}')
        return None

# Bestellungen vom Report-Tag (nur wenn nach CUTOFF)
orders_today, orders_week = [], []
if REPORT_DATE >= CONVERSION_CUTOFF:
    iso_start = f'{REPORT_DATE}T00:00:00Z'
    iso_end = f'{REPORT_DATE}T23:59:59Z'
    o = shopify_get('orders.json', {
        'created_at_min': iso_start, 'created_at_max': iso_end,
        'status': 'any', 'limit': 250,
    }) or {}
    orders_today = o.get('orders', [])

# 7-Tage-Bestellungen für Vergleich
week_min = max(WEEK_START, CONVERSION_CUTOFF)
if week_min <= WEEK_END:
    iso_start = f'{week_min}T00:00:00Z'
    iso_end = f'{WEEK_END}T23:59:59Z'
    o = shopify_get('orders.json', {
        'created_at_min': iso_start, 'created_at_max': iso_end,
        'status': 'any', 'limit': 250,
    }) or {}
    orders_week = o.get('orders', [])

def order_total(o):
    try: return float(o.get('total_price', 0))
    except: return 0.0

revenue_today = sum(order_total(o) for o in orders_today)
revenue_week_avg = sum(order_total(o) for o in orders_week) / max((WEEK_END - week_min).days + 1, 1) if orders_week else 0

# Top-SKUs vom Report-Tag
sku_counts = {}
for o in orders_today:
    for li in o.get('line_items', []):
        title = li.get('title', '?')
        qty = li.get('quantity', 0)
        sku_counts[title] = sku_counts.get(title, 0) + qty
top_skus = sorted(sku_counts.items(), key=lambda x: -x[1])[:5]

# Lagerwarnungen: Variants mit inventory_quantity < THRESHOLD (nur tracked)
print('Shopify: Low-Stock…')
low_stock = []
try:
    # Performance: nur products holen die getrackt sind, GraphQL wäre eleganter, aber REST reicht
    page_info = None
    pages_fetched = 0
    while pages_fetched < 10:  # safety: max 2500 Produkte
        params = {'limit': 250, 'fields': 'id,title,handle,variants'}
        if page_info:
            params['page_info'] = page_info
        url = f'{SHOP_API}/products.json'
        from urllib.parse import urlencode
        req = urlreq.Request(f'{url}?{urlencode(params)}', headers={
            'X-Shopify-Access-Token': SHOPIFY_TOKEN,
            'Accept': 'application/json',
        })
        with urlreq.urlopen(req, timeout=60) as r:
            data = json.loads(r.read())
            link = r.headers.get('Link', '')
        for p in data.get('products', []):
            for v in p.get('variants', []):
                if v.get('inventory_management') == 'shopify':
                    iq = v.get('inventory_quantity') or 0
                    if 0 < iq < LOW_STOCK_THRESHOLD:
                        low_stock.append({
                            'title': p['title'],
                            'sku': v.get('sku') or '',
                            'qty': iq,
                            'handle': p['handle'],
                        })
        # Pagination via Link-Header
        if 'rel="next"' in link:
            import re
            m = re.search(r'<[^>]*[?&]page_info=([^&>]+)[^>]*>; rel="next"', link)
            page_info = m.group(1) if m else None
            if not page_info: break
        else:
            break
        pages_fetched += 1
        time.sleep(0.6)  # rate-limit-Schutz
except Exception as e:
    print(f'  ⚠ Low-Stock-Scan abgebrochen: {e}')
low_stock.sort(key=lambda x: x['qty'])

# Abgebrochene Checkouts vom Vortag
print('Shopify: Abandoned Checkouts…')
abandoned_count = 0
abandoned_value = 0
try:
    iso_start = f'{REPORT_DATE}T00:00:00Z'
    iso_end = f'{REPORT_DATE}T23:59:59Z'
    ac = shopify_get('checkouts.json', {
        'created_at_min': iso_start, 'created_at_max': iso_end, 'limit': 250,
    }) or {}
    checkouts = ac.get('checkouts', [])
    abandoned_count = len(checkouts)
    abandoned_value = sum(float(c.get('total_price') or 0) for c in checkouts)
except Exception as e:
    print(f'  ⚠ Abandoned-Checkouts-Fehler: {e}')

# ── 4. Health-Check ──────────────────────────────────────────────────────────
print('Health-Check: HTTP-Pings…')
health = []
for label, url in HEALTH_URLS:
    t0 = time.time()
    try:
        req = urlreq.Request(url, headers={'User-Agent': 'Mozilla/5.0 holzagenten24-daily-report'})
        with urlreq.urlopen(req, timeout=15) as r:
            status = r.status
            health.append({'label': label, 'url': url, 'status': status, 'ms': int((time.time()-t0)*1000)})
    except urlerr.HTTPError as e:
        health.append({'label': label, 'url': url, 'status': e.code, 'ms': int((time.time()-t0)*1000), 'error': str(e)})
    except Exception as e:
        health.append({'label': label, 'url': url, 'status': 0, 'ms': int((time.time()-t0)*1000), 'error': str(e)})

# Shopify-API erreichbar?
shop_info = shopify_get('shop.json')
shop_ok = bool(shop_info and shop_info.get('shop'))
health.append({
    'label': 'Shopify Admin API',
    'url': SHOP_API,
    'status': 200 if shop_ok else 500,
    'ms': 0,
    'error': '' if shop_ok else 'Shop-Endpoint nicht erreichbar',
})

health_failed = [h for h in health if h['status'] not in (200, 301, 302)]
health_slow = [h for h in health if h.get('ms', 0) > 3000]

# ── 5. Conversion-Rate-Analyse ───────────────────────────────────────────────
def f(v):
    try: return float(v)
    except: return 0.0
def ii(v):
    try: return int(float(v))
    except: return 0

def cr(numerator, denominator):
    """Conversion-Rate als float 0.0-1.0"""
    if not denominator: return 0
    return numerator / denominator

sessions_today = ii(today_t.get('sessions'))
sessions_yest = ii(yest_t.get('sessions'))
sessions_samedaylw = ii(samedaylw_t.get('sessions'))
sessions_week = week_avg.get('sessions', 0)
sessions_prev_week = prev_week_avg.get('sessions', 0)

view_today = ev_today.get('view_item', 0)
a2c_today = ev_today.get('add_to_cart', 0)
checkout_today = ev_today.get('begin_checkout', 0)
purchase_today_ga = ev_today.get('purchase', 0)

view_yest = ev_yest.get('view_item', 0)
a2c_yest = ev_yest.get('add_to_cart', 0)
checkout_yest = ev_yest.get('begin_checkout', 0)
purchase_yest_ga = ev_yest.get('purchase', 0)

# Conversion-Rates auf Stufen-Ebene
cr_today = {
    'view_to_a2c': cr(a2c_today, view_today),
    'a2c_to_checkout': cr(checkout_today, a2c_today),
    'checkout_to_purchase': cr(purchase_today_ga, checkout_today),
    'overall': cr(purchase_today_ga, sessions_today),
}
cr_yest = {
    'view_to_a2c': cr(a2c_yest, view_yest),
    'a2c_to_checkout': cr(checkout_yest, a2c_yest),
    'checkout_to_purchase': cr(purchase_yest_ga, checkout_yest),
    'overall': cr(purchase_yest_ga, sessions_yest),
}
# 7-Tage-CRs für Benchmark
cr_week = {
    'view_to_a2c': cr(ev_week_total.get('add_to_cart', 0), ev_week_total.get('view_item', 0)),
    'a2c_to_checkout': cr(ev_week_total.get('begin_checkout', 0), ev_week_total.get('add_to_cart', 0)),
    'checkout_to_purchase': cr(ev_week_total.get('purchase', 0), ev_week_total.get('begin_checkout', 0)),
    'overall': cr(ev_week_total.get('purchase', 0), ii(week_raw.get('sessions', 0))),
}
cr_prev_week = {
    'overall': cr(ev_prev_week_total.get('purchase', 0), ii(prev_week_raw.get('sessions', 0))),
}

# Shopify-Bestellungen
purchases_today = len(orders_today)
purchases_week_per_day = len(orders_week) / max((WEEK_END - week_min).days + 1, 1) if orders_week else 0
aov_week = (sum(order_total(o) for o in orders_week) / len(orders_week)) if orders_week else 0
aov_today = (revenue_today / purchases_today) if purchases_today else 0

# ── 6. To-Dos ────────────────────────────────────────────────────────────────
todos = []

# Health: jedes nicht-erreichbare URL ist Prio 1
for h in health_failed:
    todos.append({
        'prio': 1,
        'title': f'Site-Check fehlgeschlagen: {h["label"]} ({h["status"]})',
        'detail': f'URL: {h["url"]} — {h.get("error","")}. Sofort prüfen ob Shop online ist.',
    })

# Sessions-Einbruch
if sessions_week >= 20 and sessions_today < sessions_week * 0.6:
    todos.append({
        'prio': 1,
        'title': f'Sessions eingebrochen: {sessions_today} (7-Tage-Ø: {sessions_week:.0f})',
        'detail': 'Prüfe: GSC-Sperren, Ads-Pause, neue Server-Errors, Mobile-Performance.',
    })

# Conversion-Einbruch (nur nach Cutoff)
if REPORT_DATE >= CONVERSION_CUTOFF and purchases_week_per_day >= 0.5 and purchases_today == 0:
    todos.append({
        'prio': 1,
        'title': 'Keine Bestellung gestern',
        'detail': f'7-Tage-Ø liegt bei {purchases_week_per_day:.1f}/Tag. Checkout manuell durchklicken, Zahlungsarten prüfen.',
    })

# Funnel-Drops
if view_today >= 20 and a2c_today < view_today * 0.03:
    todos.append({
        'prio': 2,
        'title': f'PDP→Warenkorb schwach: {a2c_today}/{view_today} = {a2c_today/max(view_today,1)*100:.1f}%',
        'detail': 'PDP-Optimierung: Preis sichtbar? Lieferzeit klar? CTA prominent? Mengenrabatt-Badge sichtbar?',
    })

if a2c_today >= 5 and purchases_today == 0 and REPORT_DATE >= CONVERSION_CUTOFF:
    todos.append({
        'prio': 2,
        'title': f'{a2c_today} Warenkorb-Adds aber 0 Käufe',
        'detail': f'Checkout-Friktion. {abandoned_count} abgebrochene Checkouts (Wert: {abandoned_value:.0f}€). Versandkosten? Pflichtfelder?',
    })

# Lager kritisch
if low_stock:
    sample = ', '.join(f'{s["title"][:30]} ({s["qty"]})' for s in low_stock[:3])
    todos.append({
        'prio': 2,
        'title': f'{len(low_stock)} Produkte mit Bestand <{LOW_STOCK_THRESHOLD}',
        'detail': f'Beispiele: {sample}. Bei Kahrs nachbestellen oder als nicht-lieferbar markieren.',
    })

# Bounce hoch
br = f(today_t.get('bounceRate'))
if br > 0.7 and sessions_today >= 30:
    todos.append({
        'prio': 3,
        'title': f'Bounce-Rate {br*100:.0f}% (>70%)',
        'detail': 'Landing-Page-Qualität prüfen — auffällige Pages siehe Tabelle unten.',
    })

# Slow-Pages
if health_slow:
    todos.append({
        'prio': 3,
        'title': f'{len(health_slow)} Seiten >3s Ladezeit',
        'detail': f'Performance-Backlog vorziehen falls dauerhaft.',
    })

# GSC: Impressionen ohne Klicks
high_imp_low_ctr = [r for r in gsc_top_queries if r.get('impressions', 0) >= 50 and r.get('ctr', 0) < 0.01]
if high_imp_low_ctr:
    keys = ', '.join(r['keys'][0] for r in high_imp_low_ctr[:3])
    todos.append({
        'prio': 3,
        'title': f'GSC: {len(high_imp_low_ctr)} Queries mit hohen Impressions aber <1% CTR',
        'detail': f'Top-Beispiele: {keys}. Meta-Title/Description für diese Begriffe schärfen.',
    })

# GSC: Ranking-Verlierer (Klicks-Drop > 5)
if gsc_losers and gsc_losers[0]['click_delta'] <= -3:
    top_loser = gsc_losers[0]
    todos.append({
        'prio': 2,
        'title': f'GSC: "{top_loser["query"]}" verliert Klicks ({top_loser["clicks_prev"]} → {top_loser["clicks_now"]})',
        'detail': f'Position: {top_loser["pos_now"]:.1f}. Schau ob die landing-page noch indexiert ist, evtl. Content auffrischen.',
    })

# Paid Search: Sessions aber 0 Käufe
if paid_search:
    ps_sess_week = ii(paid_search['sessions'])
    ps_pur_week = ch_purchases.get('Paid Search', 0)
    if ps_sess_week >= 15 and ps_pur_week == 0:
        todos.append({
            'prio': 2,
            'title': f'Ads: {ps_sess_week} Paid-Search-Sessions diese Woche, 0 Käufe',
            'detail': 'Google Ads prüfen: Search-Terms, Negative Keywords, Landing-Page-Qualität. → ads.google.com',
        })

# CR-Drops über Woche
if cr_prev_week['overall'] > 0.005 and cr_week['overall'] < cr_prev_week['overall'] * 0.7:
    todos.append({
        'prio': 2,
        'title': f'Conversion-Rate-Drop: {cr_week["overall"]*100:.2f}% (Vorwoche: {cr_prev_week["overall"]*100:.2f}%)',
        'detail': 'Funnel-Tabelle prüfen — wo geht die Conversion verloren?',
    })

# Mobile/Desktop-Split (haben wir noch nicht — könnte nachgezogen werden)

todos.sort(key=lambda t: t['prio'])

# ── 6. HTML rendern ──────────────────────────────────────────────────────────
def fmt(n, dec=0):
    try:
        n = float(n)
        if dec == 0: return f'{int(round(n)):,}'.replace(',', '.')
        return f'{n:,.{dec}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
    except: return str(n)
def pct(n, dec=1):
    try: return f'{float(n)*100:.{dec}f} %'.replace('.', ',')
    except: return '—'
def eur(n):
    try: return f'{float(n):,.2f} €'.replace(',', 'X').replace('.', ',').replace('X', '.')
    except: return '—'
def delta(now, prev, is_rate=False):
    try:
        now = float(now); prev = float(prev)
        if prev == 0: return '<span style="color:#888">—</span>'
        ch = (now - prev) / prev * 100
        arrow = '↑' if ch > 0 else ('↓' if ch < 0 else '→')
        good = (ch < 0) if is_rate else (ch > 0)
        color = '#1e7a3a' if good else ('#b00020' if ch != 0 else '#888')
        return f'<span style="color:{color}">{arrow} {abs(ch):.0f}%</span>'
    except: return '—'

PRIO = {
    1: ('#b00020', 'SOFORT'),
    2: ('#b85c00', 'HOCH'),
    3: ('#1e7a3a', 'MITTEL'),
}
def prio_badge(p):
    c, l = PRIO.get(p, ('#888', '?'))
    return f'<span style="background:{c};color:#fff;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:600">{l}</span>'

todo_html = ''
if todos:
    todo_html = '<ol style="padding-left:20px;margin:8px 0">'
    for t in todos:
        todo_html += f'<li style="margin:10px 0">{prio_badge(t["prio"])} <strong>{html.escape(t["title"])}</strong><br><span style="color:#555;font-size:13px">{html.escape(t["detail"])}</span></li>'
    todo_html += '</ol>'
else:
    todo_html = '<div style="color:#1e7a3a;padding:12px;background:#e8f5e9;border-radius:6px">✓ Keine Auffälligkeiten — alles im grünen Bereich.</div>'

# Health-Banner
health_banner = ''
if health_failed:
    health_banner = f'<div style="background:#fee;border:1px solid #b00020;color:#b00020;padding:10px 14px;border-radius:6px;margin:8px 0"><strong>⚠ Site-Check:</strong> {len(health_failed)} URL(s) nicht erreichbar — siehe To-Dos.</div>'
else:
    health_banner = f'<div style="background:#e8f5e9;border:1px solid #1e7a3a;color:#1e7a3a;padding:8px 14px;border-radius:6px;margin:8px 0;font-size:13px">✓ Alle {len(health)} Site-Checks OK (∅ {sum(h["ms"] for h in health)//max(len(health),1)}ms)</div>'

# Funnel-Tabelle mit Conversion-Rates auf jeder Stufe
funnel_rows = ''
funnel_data = []
for i, ev in enumerate(FUNNEL):
    c_today = ev_today.get(ev, 0)
    c_yest = ev_yest.get(ev, 0)
    c_lw = ev_samedaylw.get(ev, 0)
    c_week_avg = ev_week_avg.get(ev, 0)
    # Conversion-Rate vom vorherigen Step
    if i > 0:
        prev = ev_today.get(FUNNEL[i-1], 0)
        rate_today = cr(c_today, prev) if prev else 0
        prev_w = ev_week_total.get(FUNNEL[i-1], 0)
        rate_week = cr(ev_week_total.get(ev, 0), prev_w) if prev_w else 0
        rate_cell = f'<td style="text-align:right"><strong>{rate_today*100:.1f}%</strong><br><span style="color:#888;font-size:10px">Ø {rate_week*100:.1f}%</span></td>'
    else:
        rate_cell = '<td style="text-align:right;color:#888">—</td>'
    funnel_rows += f'<tr><td><strong>{FUNNEL_LABELS[ev]}</strong><br><span style="color:#888;font-size:10px">{ev}</span></td><td style="text-align:right">{fmt(c_today)}</td>{rate_cell}<td style="text-align:right">{fmt(c_yest)}</td><td style="text-align:right">{fmt(c_lw)}</td><td style="text-align:right">{fmt(c_week_avg,1)}</td><td style="text-align:right">{delta(c_today, c_week_avg)}</td></tr>'

# Channels mit CR
channel_rows = ''
total_ch_sessions = sum(ii(c['sessions']) for c in ch)
for c in ch:
    name = c['sessionDefaultChannelGroup']
    sess = ii(c['sessions'])
    pur = ch_purchases.get(name, 0)
    a2c = ch_a2c.get(name, 0)
    share = (sess / total_ch_sessions * 100) if total_ch_sessions else 0
    channel_cr = (pur / sess * 100) if sess else 0
    a2c_cr = (a2c / sess * 100) if sess else 0
    channel_rows += f'<tr><td><strong>{html.escape(name)}</strong></td><td style="text-align:right">{fmt(sess)}<br><span style="color:#888;font-size:10px">{share:.0f}%</span></td><td style="text-align:right">{pct(c["engagementRate"])}</td><td style="text-align:right">{fmt(a2c)}<br><span style="color:#888;font-size:10px">{a2c_cr:.2f}%</span></td><td style="text-align:right"><strong>{pur}</strong><br><span style="color:#888;font-size:10px">{channel_cr:.2f}%</span></td></tr>'

# Google Ads Sektion aus GA4 Paid-Search-Kanal
if paid_search:
    ps_sess = ii(paid_search['sessions'])
    ps_a2c = ch_a2c.get('Paid Search', 0)
    ps_co = ch_checkout_ch.get('Paid Search', 0)
    ps_pur = ch_purchases.get('Paid Search', 0)
    ps_a2c_cr = f'{ps_a2c / ps_sess * 100:.1f}%' if ps_sess else '—'
    ps_co_cr = f'{ps_co / ps_sess * 100:.1f}%' if ps_sess else '—'
    ps_pur_cr = f'{ps_pur / ps_sess * 100:.1f}%' if ps_sess else '—'
    pur_color = '#b00020' if ps_pur == 0 and ps_sess >= 10 else '#1e7a3a' if ps_pur > 0 else '#444'
    ads_section = f'''<h2>📣 Google Ads · Paid Search (letzte 7 Tage)</h2>
<table>
<tr><th>Kennzahl</th><th style="text-align:right">letzte 7 Tage</th><th style="text-align:right">CR</th></tr>
<tr><td>Sessions</td><td style="text-align:right"><strong>{fmt(ps_sess)}</strong></td><td style="text-align:right">—</td></tr>
<tr><td>Warenkorb-Adds</td><td style="text-align:right">{fmt(ps_a2c)}</td><td style="text-align:right">{ps_a2c_cr}</td></tr>
<tr><td>Checkout-Starts</td><td style="text-align:right">{fmt(ps_co)}</td><td style="text-align:right">{ps_co_cr}</td></tr>
<tr><td><strong>Käufe</strong></td><td style="text-align:right;color:{pur_color}"><strong>{fmt(ps_pur)}</strong></td><td style="text-align:right"><strong>{ps_pur_cr}</strong></td></tr>
</table>
<div class="note">Quellen: GA4-Kanal "Paid Search". Kostendaten (Spend, CPC, ROAS) nach Google Ads Developer-Token-Setup. <a href="https://ads.google.com/aw/campaigns" style="color:#2d5016">Google Ads öffnen →</a></div>'''
else:
    ads_section = '''<h2>📣 Google Ads · Paid Search</h2>
<div style="background:#fff8e6;border:1px solid #f3c969;padding:10px 14px;border-radius:6px;font-size:12px;color:#5e4500;margin:10px 0">
Keine Paid-Search-Sessions letzte 7 Tage — Kampagne aktiv? Budget erschöpft?
</div>'''

# Landing Problems
problem_lp = [p for p in lp if f(p['bounceRate']) > 0.6 and ii(p['sessions']) >= 5][:5]
lp_rows = ''.join(
    f'<tr><td><code>{html.escape(p["landingPage"][:60])}</code></td><td style="text-align:right">{fmt(p["sessions"])}</td><td style="text-align:right"><span style="color:#b00020">{pct(p["bounceRate"])}</span></td></tr>'
    for p in problem_lp
) or '<tr><td colspan="3" style="color:#666;font-style:italic">Keine auffälligen Landing-Pages</td></tr>'

# Shopify-Bereich
top_sku_rows = ''.join(
    f'<tr><td>{html.escape(title[:60])}</td><td style="text-align:right">{qty}</td></tr>'
    for title, qty in top_skus
) or '<tr><td colspan="2" style="color:#666;font-style:italic">Keine Bestellungen gestern</td></tr>'

low_stock_rows = ''.join(
    f'<tr><td><a href="{SHOP_URL}/products/{s["handle"]}" style="color:#2d5016">{html.escape(s["title"][:55])}</a></td><td>{html.escape(s["sku"])}</td><td style="text-align:right;color:#b00020"><strong>{s["qty"]}</strong></td></tr>'
    for s in low_stock[:15]
) or '<tr><td colspan="3" style="color:#666;font-style:italic">Keine kritischen Lagerstände</td></tr>'

# Health-Check Detail
health_rows = ''.join(
    f'<tr><td>{html.escape(h["label"])}</td><td><code>{html.escape(h["url"][:50])}</code></td><td style="text-align:right">{"<span style=color:#1e7a3a>"+str(h["status"])+"</span>" if h["status"] in (200,301,302) else "<span style=color:#b00020;font-weight:600>"+str(h["status"])+"</span>"}</td><td style="text-align:right">{h.get("ms",0)} ms</td></tr>'
    for h in health
)

# GSC
gsc_rows = ''.join(
    f'<tr><td>{html.escape(r["keys"][0])}</td><td style="text-align:right">{fmt(r.get("clicks",0))}</td><td style="text-align:right">{fmt(r.get("impressions",0))}</td><td style="text-align:right">{pct(r.get("ctr",0))}</td><td style="text-align:right">{r.get("position",0):.1f}</td></tr>'
    for r in gsc_top_queries[:10]
) or '<tr><td colspan="5" style="color:#666">Keine GSC-Daten (Latenz 2-3 Tage)</td></tr>'

# Shopify-KPIs
shopify_kpi_html = ''
if REPORT_DATE >= CONVERSION_CUTOFF:
    shopify_kpi_html = f'''
  <div class="kpi"><div class="v">{purchases_today}</div><div class="ch">7-T-Ø: {purchases_week_per_day:.1f}/Tag</div><div class="l">Bestellungen</div></div>
  <div class="kpi"><div class="v">{eur(revenue_today)}</div><div class="ch">7-T-Ø: {eur(revenue_week_avg)}</div><div class="l">Umsatz</div></div>
  <div class="kpi"><div class="v">{abandoned_count}</div><div class="ch">{eur(abandoned_value)}</div><div class="l">Abandoned</div></div>'''
else:
    shopify_kpi_html = f'<div class="kpi" style="grid-column:span 3"><div class="v" style="font-size:14px;color:#888">Bestellungen werden erst ab {CONVERSION_CUTOFF.strftime("%d.%m.%Y")} ausgewertet (Test-Käufe-Stichtag)</div></div>'

# ── Berechnungen für neuen Report-Block ──────────────────────────────────────
# Trend-Sparkline (28 Tage)
max_sess_t = max([int(d.get('sessions', 0)) for d in daily_trend] + [1])
max_conv_t = max(list(purchases_by_day.values()) + [1])
trend_bars = ''
report_date_str = str(REPORT_DATE).replace('-', '')
for d in daily_trend:
    dt = d['date']
    sess = int(d.get('sessions', 0))
    conv = purchases_by_day.get(dt, 0)
    h_sess = max(2, int(sess / max_sess_t * 60))
    h_conv = max(0, int(conv / max_conv_t * 30)) if conv else 0
    color_sess = '#b85c00' if dt == report_date_str else '#2d5016'
    dt_fmt = f'{dt[6:8]}.{dt[4:6]}'
    trend_bars += f'<div style="display:inline-block;width:22px;margin:0 1px;vertical-align:bottom;text-align:center" title="{dt}: {sess} Sess., {conv} Käufe"><div style="height:{h_conv}px;background:#b00020;margin-bottom:1px"></div><div style="height:{h_sess}px;background:{color_sess}"></div><div style="font-size:8px;color:#888;margin-top:2px">{dt_fmt}</div></div>'

# GSC Winner/Loser/Chancen
def gsc_change_rows(items):
    if not items: return '<tr><td colspan="4" style="color:#666;font-style:italic">Keine signifikanten Veränderungen</td></tr>'
    rows = ''
    for q in items:
        d = q['click_delta']
        col = '#1e7a3a' if d > 0 else '#b00020'
        arrow = '↑' if d > 0 else '↓'
        rows += f'<tr><td>{html.escape(q["query"])}</td><td style="text-align:right">{q["clicks_now"]}</td><td style="text-align:right;color:{col}"><strong>{arrow} {abs(d)}</strong></td><td style="text-align:right">{q["pos_now"]:.1f}</td></tr>'
    return rows

gsc_winner_rows = gsc_change_rows(gsc_winners)
gsc_loser_rows = gsc_change_rows(gsc_losers)
gsc_opp_rows = ''
for q in gsc_opportunities:
    ctr_now = q['clicks_now'] / max(q['impr_now'], 1) * 100
    gsc_opp_rows += f'<tr><td>{html.escape(q["query"])}</td><td style="text-align:right">{q["impr_now"]}</td><td style="text-align:right">{q["clicks_now"]}</td><td style="text-align:right"><span style="color:#b85c00">{ctr_now:.2f}%</span></td><td style="text-align:right">{q["pos_now"]:.1f}</td></tr>'
if not gsc_opp_rows:
    gsc_opp_rows = '<tr><td colspan="5" style="color:#666;font-style:italic">Keine Optimierungs-Kandidaten</td></tr>'

# Top-Pages
top_pages_rows = ''
for p in top_pages[:10]:
    br_p = f(p['bounceRate'])
    br_color = '#b00020' if br_p > 0.7 else ('#b85c00' if br_p > 0.5 else '#1e7a3a')
    dur = f(p.get('averageSessionDuration', 0))
    dur_min = int(dur // 60); dur_sec = int(dur % 60)
    top_pages_rows += f'<tr><td><code>{html.escape(p["pagePath"][:55])}</code></td><td style="text-align:right">{fmt(p["screenPageViews"])}</td><td style="text-align:right"><span style="color:{br_color}">{pct(br_p)}</span></td><td style="text-align:right">{dur_min}:{dur_sec:02d}</td></tr>'

# Executive Summary Tabelle
def exec_row(label, t, y, lw, w, is_int=True):
    fmt_v = (lambda v: fmt(v)) if is_int else (lambda v: f'{v:.1f}'.replace('.', ','))
    return f'<tr><td><strong>{label}</strong></td><td style="text-align:right;font-size:15px;font-weight:700;color:#2d5016">{fmt_v(t)}</td><td style="text-align:right">{fmt_v(y)}<br><span style="color:#888;font-size:10px">{delta(t, y)}</span></td><td style="text-align:right">{fmt_v(lw)}<br><span style="color:#888;font-size:10px">{delta(t, lw)}</span></td><td style="text-align:right">{fmt_v(w)}</td></tr>'

exec_rows_html = ''
exec_rows_html += exec_row('Sessions', sessions_today, sessions_yest, sessions_samedaylw, sessions_week, is_int=False)
exec_rows_html += exec_row('User', ii(today_t.get('totalUsers',0)), ii(yest_t.get('totalUsers',0)), ii(samedaylw_t.get('totalUsers',0)), week_avg.get('totalUsers',0), is_int=False)
exec_rows_html += exec_row('PDP-Views', view_today, view_yest, ev_samedaylw.get('view_item',0), ev_week_avg.get('view_item',0), is_int=False)
exec_rows_html += exec_row('Warenkorb-Adds', a2c_today, a2c_yest, ev_samedaylw.get('add_to_cart',0), ev_week_avg.get('add_to_cart',0), is_int=False)
exec_rows_html += exec_row('Checkout-Starts', checkout_today, checkout_yest, ev_samedaylw.get('begin_checkout',0), ev_week_avg.get('begin_checkout',0), is_int=False)
exec_rows_html += f'<tr><td><strong>Käufe (Shop)</strong></td><td style="text-align:right;font-size:15px;font-weight:700;color:#2d5016">{purchases_today}</td><td style="text-align:right;color:#888">—</td><td style="text-align:right;color:#888">—</td><td style="text-align:right">{purchases_week_per_day:.1f}</td></tr>'

# Conversion-Rate-Cards
def cr_color(rate):
    if rate >= 0.02: return '#1e7a3a'
    if rate >= 0.005: return '#b85c00'
    return '#b00020'

def cr_card(label, today_v, week_v, yest_v, big=False):
    border = '2px solid #2d5016' if big else '1px solid #e6e3dc'
    bg = '#fffbe8' if big else '#fff'
    return f'<div style="background:{bg};border:{border};border-radius:6px;padding:14px;text-align:center"><div style="font-size:11px;color:#666;text-transform:uppercase">{label}</div><div style="font-size:24px;font-weight:700;color:{cr_color(today_v)};margin:4px 0">{today_v*100:.2f}%</div><div style="font-size:11px;color:#888">7-T-Ø: {week_v*100:.2f}% · Vortag: {yest_v*100:.2f}%</div></div>'

cr_block = f'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:8px 0 16px">'
cr_block += cr_card('PDP → Warenkorb', cr_today['view_to_a2c'], cr_week['view_to_a2c'], cr_yest['view_to_a2c'])
cr_block += cr_card('Warenkorb → Checkout', cr_today['a2c_to_checkout'], cr_week['a2c_to_checkout'], cr_yest['a2c_to_checkout'])
cr_block += cr_card('Checkout → Kauf', cr_today['checkout_to_purchase'], cr_week['checkout_to_purchase'], cr_yest['checkout_to_purchase'])
cr_block += cr_card('Overall (Session → Kauf)', cr_today['overall'], cr_week['overall'], cr_yest['overall'], big=True)
cr_block += '</div>'

# Shopify-Sektion
shopify_section = ''
if REPORT_DATE >= CONVERSION_CUTOFF and (orders_today or orders_week):
    shopify_section = f'''<h2>🛒 Shopify · Bestellungen + Umsatz</h2>
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:8px 0 16px">
<div style="background:#fff;border:1px solid #e6e3dc;border-radius:6px;padding:14px;text-align:center"><div style="font-size:11px;color:#666;text-transform:uppercase">Bestellungen gestern</div><div style="font-size:24px;font-weight:700;color:#2d5016">{purchases_today}</div><div style="font-size:11px;color:#888">7-T-Ø: {purchases_week_per_day:.1f}/Tag</div></div>
<div style="background:#fff;border:1px solid #e6e3dc;border-radius:6px;padding:14px;text-align:center"><div style="font-size:11px;color:#666;text-transform:uppercase">Umsatz gestern</div><div style="font-size:24px;font-weight:700;color:#2d5016">{eur(revenue_today)}</div><div style="font-size:11px;color:#888">7-T-Ø: {eur(revenue_week_avg)}</div></div>
<div style="background:#fff;border:1px solid #e6e3dc;border-radius:6px;padding:14px;text-align:center"><div style="font-size:11px;color:#666;text-transform:uppercase">AOV gestern</div><div style="font-size:24px;font-weight:700;color:#2d5016">{eur(aov_today)}</div><div style="font-size:11px;color:#888">7-T-Ø: {eur(aov_week)}</div></div>
<div style="background:#fff;border:1px solid #e6e3dc;border-radius:6px;padding:14px;text-align:center"><div style="font-size:11px;color:#666;text-transform:uppercase">Abandoned</div><div style="font-size:24px;font-weight:700;color:#b85c00">{abandoned_count}</div><div style="font-size:11px;color:#888">{eur(abandoned_value)}</div></div>
</div>
<h3 style="font-size:13px;margin:14px 0 4px;color:#2d5016">Top-Bestellungen gestern</h3>
<table><tr><th>Produkt</th><th style="text-align:right">Menge</th></tr>{top_sku_rows}</table>
'''
elif REPORT_DATE < CONVERSION_CUTOFF:
    shopify_section = f'<div style="background:#fff8e6;border:1px solid #f3c969;padding:10px 14px;border-radius:6px;font-size:12px;color:#5e4500;margin:10px 0">ℹ Shopify-Bestellungen werden erst ab {CONVERSION_CUTOFF.strftime("%d.%m.%Y")} ausgewertet.</div>'
else:
    shopify_section = '<div style="background:#fff8e6;border:1px solid #f3c969;padding:10px 14px;border-radius:6px;font-size:12px;color:#5e4500;margin:10px 0">⚠ Shopify-Bestellungen nicht zugänglich — Token braucht <code>read_orders</code>+<code>read_checkouts</code> Scope.</div>'

html_out = f'''<!DOCTYPE html>
<html lang="de"><head><meta charset="UTF-8">
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#fafaf7;color:#1a1a1a;max-width:920px;margin:0 auto;padding:20px;line-height:1.5}}
h1{{margin:0 0 4px;font-size:24px;color:#2d5016}}
h2{{font-size:16px;margin:28px 0 8px;color:#2d5016;border-bottom:2px solid #2d5016;padding-bottom:4px}}
h3{{font-size:13px;margin:14px 0 4px;color:#2d5016}}
.sub{{color:#666;margin-bottom:16px;font-size:12px}}
table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e6e3dc;border-radius:6px;font-size:12px;margin:6px 0 14px}}
th,td{{padding:7px 9px;border-bottom:1px solid #f0ede4;text-align:left;vertical-align:top}}
th{{background:#f0ede4;font-weight:600;font-size:10px;text-transform:uppercase;letter-spacing:.5px}}
code{{font-family:'SF Mono',Menlo,monospace;background:#f0ede4;padding:1px 5px;border-radius:3px;font-size:11px}}
.chart{{background:#fff;border:1px solid #e6e3dc;border-radius:6px;padding:14px;margin:8px 0 16px;overflow-x:auto}}
.legend{{font-size:10px;color:#888;margin-top:6px}}
.legend span{{display:inline-block;width:10px;height:10px;vertical-align:middle;margin-right:4px}}
.note{{font-size:11px;color:#666;margin:-6px 0 14px}}
</style></head><body>

<h1>Holzagenten24 · Daily Report</h1>
<div class="sub">Reporting-Tag: <strong>{REPORT_DATE.strftime('%A, %d.%m.%Y')}</strong> · Vergleiche: Vortag · gleicher Wochentag Vorwoche · 7-Tage-Ø</div>

{health_banner}

<h2>🎯 Handlungsempfehlungen</h2>
{todo_html}

<h2>📊 Executive Summary</h2>
<table>
<tr><th>Kennzahl</th><th style="text-align:right">Gestern</th><th style="text-align:right">Vortag</th><th style="text-align:right">Gleicher Tag<br>Vorwoche</th><th style="text-align:right">7-T-Ø</th></tr>
{exec_rows_html}
</table>

<h2>💡 Conversion-Rates</h2>
{cr_block}

<h2>📈 28-Tage-Trend (Sessions + Käufe)</h2>
<div class="chart">{trend_bars}<div class="legend"><span style="background:#2d5016"></span>Sessions &nbsp; <span style="background:#b00020"></span>Käufe &nbsp; <span style="background:#b85c00"></span>Reporting-Tag</div></div>

<h2>🔻 Funnel-Analyse mit Conversion-Rates pro Stufe</h2>
<table>
<tr><th>Stufe</th><th style="text-align:right">Gestern</th><th style="text-align:right">CR von vorh. Stufe<br><span style="font-weight:400">(7-T-Ø darunter)</span></th><th style="text-align:right">Vortag</th><th style="text-align:right">Gleicher Tag<br>Vorwoche</th><th style="text-align:right">7-T-Ø</th><th style="text-align:right">Δ ggü. Ø</th></tr>
{funnel_rows}
</table>

{shopify_section}

{ads_section}

<h2>📦 Lager-Warnungen (Bestand &lt; {LOW_STOCK_THRESHOLD})</h2>
<table><tr><th>Produkt</th><th>SKU</th><th style="text-align:right">Bestand</th></tr>{low_stock_rows}</table>

<h2>🚪 Channels (letzte 7 Tage, sortiert nach Sessions)</h2>
<table>
<tr><th>Channel</th><th style="text-align:right">Sessions<br><span style="font-weight:400">(Anteil)</span></th><th style="text-align:right">Engagement</th><th style="text-align:right">Warenkorb<br><span style="font-weight:400">(CR)</span></th><th style="text-align:right">Käufe<br><span style="font-weight:400">(CR)</span></th></tr>
{channel_rows or '<tr><td colspan="5" style="color:#666">Keine Daten</td></tr>'}
</table>

<h2>📄 Top-Pages (7 Tage)</h2>
<table>
<tr><th>URL</th><th style="text-align:right">Views</th><th style="text-align:right">Bounce</th><th style="text-align:right">Ø Dauer</th></tr>
{top_pages_rows or '<tr><td colspan="4" style="color:#666">Keine Daten</td></tr>'}
</table>

<h2>⚠️ Auffällige Landing-Pages gestern (Bounce &gt; 60%)</h2>
<table>
<tr><th>URL</th><th style="text-align:right">Sessions</th><th style="text-align:right">Bounce</th></tr>
{lp_rows}
</table>

<h2>🔍 GSC · Top Queries (letzte 7 Tage)</h2>
<table>
<tr><th>Query</th><th style="text-align:right">Klicks</th><th style="text-align:right">Impr.</th><th style="text-align:right">CTR</th><th style="text-align:right">Pos.</th></tr>
{gsc_rows}
</table>

<h2>📈 GSC · Gewinner gegenüber Vorwoche</h2>
<table>
<tr><th>Query</th><th style="text-align:right">Klicks jetzt</th><th style="text-align:right">Δ Klicks</th><th style="text-align:right">Pos.</th></tr>
{gsc_winner_rows}
</table>

<h2>📉 GSC · Verlierer gegenüber Vorwoche</h2>
<table>
<tr><th>Query</th><th style="text-align:right">Klicks jetzt</th><th style="text-align:right">Δ Klicks</th><th style="text-align:right">Pos.</th></tr>
{gsc_loser_rows}
</table>

<h2>💎 GSC · Optimierungs-Kandidaten</h2>
<table>
<tr><th>Query</th><th style="text-align:right">Impr.</th><th style="text-align:right">Klicks</th><th style="text-align:right">CTR</th><th style="text-align:right">Pos.</th></tr>
{gsc_opp_rows}
</table>
<div class="note">→ Diese Queries ranken bereits, bringen Impressionen, aber niemand klickt. Meta-Title + Description gezielt für diese Begriffe schärfen.</div>

<h2>🩺 Site-Health-Check</h2>
<table>
<tr><th>Check</th><th>URL</th><th style="text-align:right">Status</th><th style="text-align:right">Zeit</th></tr>
{health_rows}
</table>

<div style="margin-top:24px;padding-top:12px;border-top:1px solid #e6e3dc;font-size:11px;color:#888">
Erstellt {datetime.now().strftime('%Y-%m-%d %H:%M')} · GA4-Latenz ~24h, GSC ~2-3 Tage · Stichtag echte Käufe: {CONVERSION_CUTOFF.strftime('%d.%m.%Y')}
</div>

</body></html>'''

# ── 7. Versand ───────────────────────────────────────────────────────────────
if args.dry_run:
    out_path = os.path.join(HERE, f'daily_report_{REPORT_DATE}.html')
    open(out_path, 'w').write(html_out)
    print(f'✓ Dry-run: {out_path}')
    sys.exit(0)

prio1 = sum(1 for t in todos if t['prio'] == 1)
prio2 = sum(1 for t in todos if t['prio'] == 2)
date_short = REPORT_DATE.strftime('%d.%m.')
if prio1:
    subject = f'🔥 Holzagenten24 · {prio1} SOFORT-To-Do · {date_short}'
elif prio2:
    subject = f'⚠️ Holzagenten24 · {prio2} To-Dos · {date_short}'
else:
    subject = f'✓ Holzagenten24 · Daily Report · {date_short}'

msg = MIMEMultipart('alternative')
msg['Subject'] = subject
msg['From'] = REPORT_FROM
msg['To'] = REPORT_TO
msg.attach(MIMEText(html_out, 'html', 'utf-8'))

ctx = ssl.create_default_context()
with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx) as s:
    s.login(SMTP_USER, SMTP_PASSWORD)
    s.sendmail(REPORT_FROM, [REPORT_TO], msg.as_string())

print(f'✓ Mail an {REPORT_TO} gesendet — {len(todos)} To-Dos')
