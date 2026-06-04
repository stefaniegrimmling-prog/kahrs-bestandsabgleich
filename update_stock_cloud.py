#!/usr/bin/env python3
"""Täglicher Sync Kahrs → Shopify: Bestand + 3-Tier-Sichtbarkeit.

Ersetzt update_stock.py (das nur Teilmenge syncte und Pagination-Bug hatte).

Für JEDES Shopify-Produkt (active + draft):
  1. SKU-Stem → Kahrs-Daten (Lagerbestand-Summe über alle Längen-Varianten,
     Vorrat-Flag, Abverkauf-Flag).
  2. Pro Variante: Shopify-Bestand = Kahrs Lagerbestand (nur wenn verändert).
  3. Tier-Klassifikation:
        Kahrs-Bestand > 0             → Tier 1 LAGER
        Bestand=0 & Vorrat=TRUE       → Tier 2 VORRAT
        Bestand=0 & sonst             → Tier 3 ABVERKAUF
  4. Produkt-State gemäß Tier setzen (status, inventory_policy, Tags).

Escape-Hatches (nichts ändern):
  - Produkt hat Tag 'manual-keep'          → komplett überspringen
  - Produkt hat Tag 'muster'               → komplett überspringen (Muster-Logik)
  - Draft OHNE 'auto-hidden-stock'-Tag     → nicht reaktivieren (User prüft)

Modi:
  python3 stock_sync.py --dry-run   # Nur anzeigen, was passieren würde
  python3 stock_sync.py             # Live
"""
import csv, json, os, sys, time, urllib.request, urllib.error
from datetime import datetime
from collections import defaultdict

SCRIPT_DIR=os.path.dirname(os.path.abspath(__file__))
KAHRS_CSV=os.path.join(SCRIPT_DIR,'kahrs_source.csv')
LOG_DIR=SCRIPT_DIR  # Cloud: log ins Repo-Root (für GitHub-Actions-Artifact)
KAHRS_URL=os.environ.get('KAHRS_CSV_URL','https://holz-kahrs.de/media/export_data/holz_kahrs-983c3908.csv')

TAG_VORRAT='lieferzeit-14-tage'
TAG_HIDDEN='auto-hidden-stock'
TAG_HIDDEN_SORT='auto-hidden-sortiment'
TAG_MANUAL='manual-keep'

# Sortimente, die NIEMALS aktiv im Shop sein sollen
BLOCKED_SORTIMENTE={'Auslauf','Restposten','Anfall','Anfrage','Ex_Artikel'}
# Sortimente mit längerer Lieferzeit (Streckengeschäft)
KOMMISSION_SORTIMENTE={'Kommission'}

def log(msg):
    ts=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line=f"[{ts}] {msg}"
    print(line,flush=True)
    os.makedirs(LOG_DIR,exist_ok=True)
    with open(os.path.join(LOG_DIR,'stock_update.log'),'a',encoding='utf-8') as f:
        f.write(line+"\n")

def load_env():
    """Cloud-Version: liest aus os.environ (GitHub-Actions-Secrets)."""
    env={}
    for k in ('SHOPIFY_STORE','SHOPIFY_ACCESS_TOKEN'):
        v=os.environ.get(k,'').strip()
        if not v:
            print(f"FEHLER: Env-Variable {k} fehlt"); sys.exit(1)
        env[k]=v
    return env

def api(env, endpoint, method='GET', data=None, _attempt=0):
    url=f"https://{env['SHOPIFY_STORE']}/admin/api/2024-01/{endpoint}"
    headers={'X-Shopify-Access-Token':env['SHOPIFY_ACCESS_TOKEN'],'Content-Type':'application/json'}
    body=json.dumps(data).encode('utf-8') if data else None
    req=urllib.request.Request(url,data=body,headers=headers,method=method)
    try:
        with urllib.request.urlopen(req,timeout=30) as resp:
            link=resp.headers.get('Link','')
            limit=resp.headers.get('X-Shopify-Shop-Api-Call-Limit','')
            if limit:
                used,total=[int(x) for x in limit.split('/')]
                if used >= total - 2:
                    time.sleep(1.0)  # bucket fast voll → atmen
            return json.loads(resp.read().decode('utf-8')), link
    except urllib.error.HTTPError as e:
        body=e.read().decode('utf-8') if e.fp else ''
        if e.code == 429 and _attempt < 5:
            wait = float(e.headers.get('Retry-After', '2')) * (1 + _attempt)
            time.sleep(wait)
            return api(env, endpoint, method, data, _attempt+1)
        if e.code in (500, 502, 503, 504) and _attempt < 3:
            time.sleep(2 * (_attempt+1))
            return api(env, endpoint, method, data, _attempt+1)
        log(f"  API {e.code} {endpoint[:60]}: {body[:150]}")
        return None, ''
    except (urllib.error.URLError, TimeoutError, ConnectionResetError, OSError) as e:
        if _attempt < 5:
            time.sleep(3 * (_attempt+1))
            return api(env, endpoint, method, data, _attempt+1)
        log(f"  NET {endpoint[:60]}: {e}")
        return None, ''
    except Exception as e:
        # alles andere: ein Versuch noch, dann sauber None zurueck
        if _attempt < 2:
            time.sleep(5)
            return api(env, endpoint, method, data, _attempt+1)
        log(f"  EXC {endpoint[:60]}: {type(e).__name__}: {e}")
        return None, ''

def download_kahrs():
    log(f"Lade Kahrs-CSV von {KAHRS_URL[:70]} ...")
    last_err=None
    for attempt in range(3):
        try:
            req=urllib.request.Request(KAHRS_URL, headers={'User-Agent':'Mozilla/5.0'})
            with urllib.request.urlopen(req,timeout=120) as resp:
                data = resp.read()
            if len(data) < 10000:
                raise ValueError(f"CSV unrealistisch klein: {len(data)} bytes")
            with open(KAHRS_CSV, 'wb') as f:
                f.write(data)
            log(f"Download OK ({len(data)} bytes).")
            return
        except Exception as e:
            last_err=e
            log(f"  Download-Fehler (Versuch {attempt+1}/3): {e}")
            time.sleep(5 * (attempt+1))
    raise RuntimeError(f"Kahrs-CSV-Download fehlgeschlagen nach 3 Versuchen: {last_err}")

def parse_kahrs():
    """Liefert:
       sku_to_qty: {full_sku → lager_int}
       stem_to_info: {sku_stem → {'lager_sum','vorrat','abverkauf'}}
    """
    sku_qty={}
    stem=defaultdict(lambda:{'lager_sum':0,'vorrat':False,'abverkauf':False})
    with open(KAHRS_CSV,'r',encoding='utf-8-sig') as f:
        for row in csv.DictReader(f, delimiter=';', quotechar='"'):
            num=(row.get('Nummer') or '').strip()
            if not num: continue
            try: qty=int((row.get('Lagerbestand') or '0').replace(',','').strip() or 0)
            except: qty=0
            if qty<0: qty=0  # Kahrs-Überverkauf (negativ) → im Shop als 0 führen
            sku_qty[num]=qty
            st=num.split('.')[0]
            stem[st]['lager_sum']+=qty
            if (row.get('Vorrat') or '').upper().strip()=='TRUE':    stem[st]['vorrat']=True
            if (row.get('Abverkauf') or '').upper().strip()=='TRUE': stem[st]['abverkauf']=True
            srt=(row.get('Sortiment') or '').strip()
            # Erstes Sortiment je Stem festhalten (genug für Klassifikation)
            if 'sortiment' not in stem[st] or not stem[st].get('sortiment'):
                stem[st]['sortiment']=srt
    return sku_qty, dict(stem)

def fetch_all_products(env):
    """Paginiert korrekt via Link-Header."""
    products=[]; endpoint='products.json?limit=250'
    while endpoint:
        result, link = api(env, endpoint)
        if not result: break
        products.extend(result.get('products',[]))
        endpoint=None
        if 'rel="next"' in link:
            for part in link.split(','):
                if 'rel="next"' in part:
                    url=part.split(';')[0].strip().strip('<>')
                    # endpoint relativ ab /admin/api/2024-01/
                    endpoint=url.split('/admin/api/2024-01/')[-1]
        time.sleep(0.3)
    return products

def get_location_id(env):
    r,_=api(env,'locations.json')
    return r['locations'][0]['id'] if r and r.get('locations') else None

def set_inventory(env, inv_item_id, loc_id, qty, dry):
    if dry: return True
    r,_=api(env,'inventory_levels/set.json','POST',{'location_id':loc_id,'inventory_item_id':inv_item_id,'available':qty})
    return r is not None

def update_variant_policy(env, vid, policy, dry):
    if dry: return True
    r,_=api(env,f'variants/{vid}.json','PUT',{'variant':{'id':vid,'inventory_policy':policy}})
    return r is not None

def update_product(env, pid, fields, dry):
    if dry: return True
    body={'product':{'id':pid, **fields}}
    r,_=api(env,f'products/{pid}.json','PUT',body)
    return r is not None

def get_delivery_metafield(env, pid):
    time.sleep(0.5)  # API rate limit (Shopify: 2/s)
    r,_=api(env,f'products/{pid}/metafields.json?namespace=custom&key=delivery_time')
    if not r: return None,None
    mfs=r.get('metafields',[])
    if not mfs: return None,None
    return mfs[0].get('id'), mfs[0].get('value')

def set_delivery_metafield(env, pid, value, dry):
    """Setzt/aktualisiert custom.delivery_time. Skipped wenn Wert unverändert."""
    cur_id, cur_val = get_delivery_metafield(env, pid)
    if cur_val==value: return False  # unverändert
    if dry: return True
    if cur_id:
        r,_=api(env,f'metafields/{cur_id}.json','PUT',{'metafield':{'id':cur_id,'value':value,'type':'single_line_text_field'}})
    else:
        payload={'metafield':{'namespace':'custom','key':'delivery_time','type':'single_line_text_field','value':value}}
        r,_=api(env,f'products/{pid}/metafields.json','POST',payload)
    return r is not None

def merge_tags(tags_csv, add=None, remove=None):
    tags=[t.strip() for t in (tags_csv or '').split(',') if t.strip()]
    if remove:
        tags=[t for t in tags if t not in remove]
    if add:
        for a in add:
            if a not in tags: tags.append(a)
    return ', '.join(tags)

def classify(stock_product_sum, kahrs_info):
    """Tier aus Summe der Shopify-Varianten-Bestände + Kahrs-Flags + Sortiment.
    Gibt (tier, target_policy, add_tags, remove_tags, target_status, delivery_time)."""
    vorrat=kahrs_info['vorrat']
    sortiment=kahrs_info.get('sortiment','')

    # Sortiment-Blockade: immer draft, egal wieviel Lager
    if sortiment in BLOCKED_SORTIMENTE:
        return (3,'deny',[TAG_HIDDEN_SORT],[TAG_VORRAT,TAG_HIDDEN],'draft','')

    is_kommission = sortiment in KOMMISSION_SORTIMENTE
    dt = '3-4 Wochen' if is_kommission else '1-2 Wochen'

    if stock_product_sum>0:
        return (1,'deny',[],[TAG_VORRAT,TAG_HIDDEN,TAG_HIDDEN_SORT],'active',dt)
    if vorrat:
        return (2,'continue',[TAG_VORRAT],[TAG_HIDDEN,TAG_HIDDEN_SORT],'active',dt)
    return (3,'deny',[TAG_HIDDEN],[TAG_VORRAT,TAG_HIDDEN_SORT],'draft','')

def main():
    dry='--dry-run' in sys.argv
    mode='DRY RUN' if dry else 'LIVE'
    log(f"=== Stock-Sync gestartet ({mode}) ===")

    env=load_env()
    download_kahrs()
    sku_qty, stem_info = parse_kahrs()
    log(f"Kahrs: {len(sku_qty)} SKUs, {len(stem_info)} Stems")

    loc_id=get_location_id(env)
    if not loc_id:
        log("FEHLER: keine Location"); sys.exit(1)
    log(f"Location: {loc_id}")

    products=fetch_all_products(env)
    log(f"Shopify: {len(products)} Produkte geladen")

    stats=defaultdict(int)
    tier_count=defaultdict(int)
    inv_updates=0; inv_skipped=0
    prod_updates=0; var_updates=0; mf_updates=0
    skipped_manual=0; skipped_no_kahrs=0; skipped_draft_keep=0
    auto_drafted_no_csv=0; auto_drafted_sortiment=0

    for p in products:
        pid=p['id']; handle=p['handle']; status=p['status']
        tags_raw=p.get('tags','') or ''
        tags=[t.strip() for t in tags_raw.split(',') if t.strip()]

        if TAG_MANUAL in tags:
            skipped_manual+=1; continue
        # Muster werden NIE über stock_sync verwaltet (kostenlos, kein Bestand-Konzept)
        if 'muster' in tags:
            skipped_manual+=1; continue

        # Kahrs-Match per erster Variante (alle Varianten teilen Stem)
        variants=p.get('variants',[])
        if not variants: continue
        first_sku=(variants[0].get('sku','') or '').strip()
        stem_key=first_sku.split('.')[0]
        kinfo=stem_info.get(stem_key)
        if not kinfo:
            # Nicht mehr in Kahrs-CSV → auf draft setzen (sofern aktiv)
            skipped_no_kahrs+=1
            if status=='active':
                new_tags=merge_tags(tags_raw, add=[TAG_HIDDEN_SORT], remove=[])
                fields={'status':'draft','tags':new_tags}
                if update_product(env,pid,fields,dry):
                    auto_drafted_no_csv+=1
                    log(f"  DRAFT-NO-CSV {handle[:55]}")
                time.sleep(0.3 if not dry else 0)
            continue

        # Summe Bestand NUR über Shopify-Varianten (Kahrs-Wert wenn bekannt,
        # sonst aktueller Shopify-Wert — für manuell angelegte Varianten ohne Kahrs-Eintrag).
        prod_stock_sum=0
        for v in variants:
            vsku=(v.get('sku','') or '').strip()
            kq=sku_qty.get(vsku)
            target_qty = kq if kq is not None else (v.get('inventory_quantity') or 0)
            prod_stock_sum += target_qty

        tier, target_policy, add_t, rem_t, target_status, delivery_time = classify(prod_stock_sum, kinfo)
        tier_count[tier]+=1
        if kinfo.get('sortiment','') in BLOCKED_SORTIMENTE:
            auto_drafted_sortiment+=1

        # Escape: Draft ohne Auto-Tag NICHT aktivieren (User hat manuell entschieden)
        if status=='draft' and TAG_HIDDEN not in tags and TAG_HIDDEN_SORT not in tags and target_status=='active':
            skipped_draft_keep+=1
            continue

        # --- 1. Varianten-Bestand synchen ---
        for v in variants:
            vsku=(v.get('sku','') or '').strip()
            kq=sku_qty.get(vsku)
            if kq is None: continue  # Variante nicht in Kahrs
            cur=v.get('inventory_quantity') or 0
            if cur==kq:
                inv_skipped+=1
                continue
            inv_id=v.get('inventory_item_id')
            if inv_id and set_inventory(env,inv_id,loc_id,kq,dry):
                inv_updates+=1
                log(f"  INV {handle[:45]:45} {vsku:25} {cur:4d} → {kq:4d}")
            time.sleep(0.2 if not dry else 0)

        # --- 2. Varianten-Policy synchen ---
        # Bei Tier 1 + Kahrs-Vorrat=TRUE: per Variant entscheiden (Mixed-Stock-Fix).
        # Variants mit Bestand=0 sollen weiterhin als Vorrat bestellbar sein (continue),
        # Variants mit Bestand>0 bleiben bei deny (kein Überverkauf der Lagerware).
        per_variant = (tier == 1 and kinfo.get('vorrat'))
        for v in variants:
            vsku = (v.get('sku','') or '').strip()
            kq = sku_qty.get(vsku)
            var_qty = kq if kq is not None else (v.get('inventory_quantity') or 0)
            if per_variant:
                v_policy = 'deny' if var_qty > 0 else 'continue'
            else:
                v_policy = target_policy
            if v.get('inventory_policy') != v_policy:
                if update_variant_policy(env,v['id'],v_policy,dry):
                    var_updates+=1
                time.sleep(0.2 if not dry else 0)

        # --- 3. Produkt Status + Tags ---
        new_tags=merge_tags(tags_raw, add=add_t, remove=rem_t)
        status_change=(status!=target_status)
        tags_change=(new_tags!=tags_raw.strip().rstrip(','))
        # normalisieren: vergleich auf Set-Ebene
        if set(t.strip() for t in new_tags.split(',') if t.strip()) == set(tags):
            tags_change=False
        if status_change or tags_change:
            fields={}
            if status_change: fields['status']=target_status
            if tags_change:   fields['tags']=new_tags
            if update_product(env,pid,fields,dry):
                prod_updates+=1
                log(f"  PRD T{tier} {handle[:50]:50} {' '.join(f'{k}={v}' for k,v in fields.items())[:80]}")
            time.sleep(0.3 if not dry else 0)

        # --- 4. Lieferzeit-Metafield ---
        if delivery_time and target_status=='active':
            if set_delivery_metafield(env, pid, delivery_time, dry):
                mf_updates+=1
                log(f"  MF  {handle[:55]:55} delivery_time={delivery_time}")
                time.sleep(0.2 if not dry else 0)

    log("")
    log("=== Zusammenfassung ===")
    log(f"  Tier 1 LAGER:     {tier_count[1]}")
    log(f"  Tier 2 VORRAT:    {tier_count[2]}")
    log(f"  Tier 3 ABVERKAUF: {tier_count[3]}")
    log(f"  Bestand-Updates:  {inv_updates} (unverändert: {inv_skipped})")
    log(f"  Variant-Policy:   {var_updates}")
    log(f"  Produkt-Updates:  {prod_updates}")
    log(f"  Lieferzeit-MF:    {mf_updates}")
    log(f"  Auto-Draft (Sortiment blockiert): {auto_drafted_sortiment}")
    log(f"  Auto-Draft (nicht in CSV):        {auto_drafted_no_csv}")
    log(f"  Übersprungen (manual-keep): {skipped_manual}")
    log(f"  Übersprungen (draft, kein Auto-Tag): {skipped_draft_keep}")
    log(f"  Übersprungen (SKU nicht in Kahrs, war schon draft): {skipped_no_kahrs-auto_drafted_no_csv}")
    log(f"=== Stock-Sync beendet ({mode}) ===\n")

if __name__=='__main__':
    main()
