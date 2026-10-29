#!/usr/bin/env python3
"""Ganze Pakete (Kahrs-PK-Partien) -- die EINZIGE Stelle fuer Paket-Gruppen, -Bestand und -Preis.

Seit 28.10.2026, Konzept: KONZEPT_PAKETE.md. Dieses Modul schreibt NICHTS in den Shop.

Kahrs verkauft viele Laengen zusaetzlich als ganzes Paket: eigene CSV-Zeile mit Nummer
`<Laengennummer>.PK-<Los>`, `Lagerbestand` = Dielen im Paket (STUECK, auch wenn
Basiseinheit lfm ist), nur komplett kaufbar. Jedes Los gibt es genau einmal.

Im Shop fuehren wir keine einzelnen Lose, sondern je Sorte (gleiche Laenge + gleiche
Dielenzahl) EINE Variante mit SKU `<Laengennummer>-PAKET-<Dielen>`. Beide Teile stehen so in
der CSV, es wird nichts gerechnet (feedback_sku_nie_konstruieren).

Stefanies Regeln (28.10.2026):
  1. Bestand = Anzahl gleicher Lose - 1. Das letzte Los ist Puffer, weil Kahrs und HHD
     dieselben Lose parallel verkaufen und wir nur zweimal am Tag abgleichen.
     Ab 2 gleichen Losen bestellbar; beim letzten Los nur auf Anfrage.
  2. Preis = Kahrs-Paketpreis x 1,40 x 0,98 brutto. 1,40 ist HHDs regulaerer Paketfaktor,
     am 28.10. an 7 Produkten centgenau abgelesen (output/hhd_pakete_20261028.json).
     HHD-Aktionen (Black Week) gehen wir NICHT mit. Nie unter EK + 3 %.
  3. Nur Lose, die im Einkauf guenstiger sind als die Einzelware derselben Laenge.
"""
import csv
import math
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
KAHRS_CSV = os.path.join(HERE, 'kahrs_source.csv')

VAT = 1.19
MIN_MARGE = 0.03          # wie preis_engine.MIN_MARGE
HHD_PAKET_FAKTOR = 1.40   # HHD brutto / Kahrs-Paket-EK netto, regulaer (28.10.2026)
ABSTAND_WETTBEWERB = 0.02 # wie preis_engine.ABSTAND_WETTBEWERB
PUFFER = 1                # so viele Lose je Sorte bleiben unverkauft

SKU_RE = re.compile(r'^(?P<basis>.+)-PAKET-(?P<dielen>\d+)$')


def ist_paket_sku(sku):
    return bool(SKU_RE.match((sku or '').strip()))


def basis_nummer(sku):
    """'18-202391.0180000000-PAKET-99' -> '18-202391.0180000000', sonst None."""
    m = SKU_RE.match((sku or '').strip())
    return m.group('basis') if m else None


def paket_sku(basis, dielen):
    return f'{basis}-PAKET-{int(dielen)}'


def _num(v):
    try:
        return float(str(v).replace(',', '.'))
    except (TypeError, ValueError):
        return 0.0


def _ek(preish):
    """Erste Staffel aus preisH ('1:8.79|100:8.35') = Einzelpreis netto."""
    for part in (preish or '').split('|'):
        if ':' in part:
            q, p = part.split(':', 1)
            if _num(q) == 1:
                return _num(p)
    return 0.0


def paket_preis(ek_lfm, lfm):
    """Bruttopreis eines ganzen Pakets nach Regel 2, nie unter EK + 3 %."""
    ziel = round(ek_lfm * lfm * HHD_PAKET_FAKTOR * (1 - ABSTAND_WETTBEWERB), 2)
    untergrenze = math.ceil(ek_lfm * lfm * (1 + MIN_MARGE) * VAT * 100 - 1e-9) / 100
    return max(ziel, untergrenze)


def lade_sorten(pfad=KAHRS_CSV):
    """Alle Paket-Sorten aus der CSV: {paket_sku: info}. Auch Sorten mit nur einem Los
    (Bestand 0, 'letztes'), damit der Bestand-Sync eine Variante beim letzten Los auf 0 setzt.

    Nur Basiseinheit lfm: bei m²-Ware (49 Lose, Innendielen) fehlt eine gepruefte
    Flaechenrechnung je Paket, die Sorten werden ausgelassen."""
    zeilen = {}
    lose = []
    with open(pfad, encoding='utf-8-sig', newline='') as f:
        for r in csv.DictReader(f, delimiter=';'):
            nr = (r.get('Nummer') or '').strip()
            if not nr:
                continue
            zeilen[nr] = r
            if '.PK-' in nr:
                lose.append(r)

    sorten = {}
    for r in lose:
        nr = r['Nummer'].strip()
        basis = nr.split('.PK-')[0]
        dielen = _num(r.get('Lagerbestand'))
        if dielen <= 0 or dielen != int(dielen):
            continue
        if (r.get('Basiseinheit') or '').strip().lower() != 'lfm':
            continue
        laenge_m = _num(r.get('Länge')) / 1000
        ek_los = _ek(r.get('preisH'))
        haupt = zeilen.get(basis)
        ek_einzel = _ek(haupt.get('preisH')) if haupt else 0.0
        if not haupt or laenge_m <= 0 or ek_los <= 0 or ek_einzel <= 0:
            continue
        if ek_los >= ek_einzel:
            continue   # Regel 3: Paket nicht guenstiger als einzeln
        sku = paket_sku(basis, dielen)
        s = sorten.get(sku)
        if s is None:
            lfm = round(int(dielen) * laenge_m, 2)
            s = sorten[sku] = {
                'sku': sku, 'basis': basis, 'dielen': int(dielen), 'laenge_m': laenge_m,
                'lfm': lfm, 'ek_lfm': ek_los, 'ek_einzel_lfm': ek_einzel,
                'gewicht_kg': round(_num(r.get('Gewicht')) * lfm, 1),
                'name': (r.get('Name') or '').strip(), 'lose': [],
            }
        elif abs(s['ek_lfm'] - ek_los) > 0.005:
            # Gleiche Sorte, anderer Los-Preis: billigsten Preis nie unterbieten
            s['ek_lfm'] = max(s['ek_lfm'], ek_los)
        s['lose'].append(nr)

    for s in sorten.values():
        s['anzahl'] = len(s['lose'])
        s['bestand'] = max(s['anzahl'] - PUFFER, 0)
        s['letztes'] = s['anzahl'] == 1
        s['preis'] = paket_preis(s['ek_lfm'], s['lfm'])
    return sorten


if __name__ == '__main__':
    import sys
    sorten = lade_sorten()
    bestellbar = [s for s in sorten.values() if s['bestand'] > 0]
    print(f'{len(sorten)} Paket-Sorten, {len(bestellbar)} bestellbar '
          f'({sum(s["bestand"] for s in bestellbar)} Pakete)')
    for sku in sys.argv[1:]:
        print(sorten.get(sku))
