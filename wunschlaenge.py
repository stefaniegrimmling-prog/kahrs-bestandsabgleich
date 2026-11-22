#!/usr/bin/env python3
"""Wunschlängen für Pferdestallbohlen (Stefanie 21.11.2026: „exakt wie HHD“).

Kahrs führt die Stallbohlen als m²-Ware OHNE Länge (Kommission, Zuschnitt auf Maß, Kahrs-Text:
„Auf Wunsch sind Zuschnitte auf Maß möglich“). Kahrs und HHD rechnen die Wunschlänge exakt nach
m² ab, ohne Zuschnittaufschlag (Kahrs-Warenkorb 20.11.2026: 34 × 1.300 mm = 5,3 m² × 56,25 €).

Umsetzung: Jede Länge von 0,50 bis 5,45 m in 50-mm-Schritten wird eine eigene Shop-Variante
„1 Bohle“ (100 je Produkt, mehr liefert die REST-API 2024-01 unserer Scripts nicht). Damit alle
Programme (Preis-Engine, Bestand, Versand, Spiegel, Audits) diese Längen wie normale Kahrs-Längen
sehen, ergänzt `erweitere_csv()` direkt nach jedem Download von kahrs_source.csv je Stamm eine
Zeile pro Länge. Nummer im Kahrs-Format (Stamm.LLLL00000, 1.300 mm -> 18-220373.0130000000),
alle anderen Felder 1:1 von der Kahrs-Zeile des Stamms. Die Stammzeile selbst bleibt.

Bereich 0,50 bis 5,45 m: Stefanie 21.11.2026 (Kahrs nennt keinen Bereich, Bittner bestätigt).
Aufgerufen von: stock_sync.download_kahrs (lokal) und github-repo/update_stock_cloud.py (Cloud).
"""
import csv, io, os

LAENGEN_MM = list(range(500, 5451, 50))          # 100 Längen
MARKER = 'Wunschlänge'                           # in Schlüsselwörtern der ergänzten Zeilen


def ist_stamm(row):
    """Pferdestallbohle als m²-Ware ohne Länge = Zuschnitt auf Maß."""
    return ('Pferdestallbohl' in (row.get('Name') or '')
            and (row.get('Basiseinheit') or '').strip() == 'm²'
            and '.' not in (row.get('Nummer') or '')
            and _zahl(row.get('Länge')) == 0 and _zahl(row.get('Breite')) > 0)


def _zahl(s):
    try:
        return float((s or '0').replace(',', '.'))
    except ValueError:
        return 0.0


def sku(stamm, laenge_mm):
    return f'{stamm}.{laenge_mm * 100000:010d}'


def variante_titel(laenge_mm):
    """Gleiche Schreibweise wie die übrigen Längen-Varianten im Shop: „1,30 m“."""
    return f'{laenge_mm / 1000:.2f} m'.replace('.', ',')


def ergaenze(rows):
    """Liste von CSV-Zeilen (dict) -> Liste inkl. Wunschlängen-Zeilen. Idempotent."""
    vorhanden = {(r.get('Nummer') or '').strip() for r in rows}
    neu = []
    for r in rows:
        if not ist_stamm(r):
            continue
        st = r['Nummer'].strip()
        for L in LAENGEN_MM:
            n = sku(st, L)
            if n in vorhanden:
                continue
            z = dict(r)
            z['Nummer'] = n
            z['Länge'] = f'{L}.000'
            kw = (z.get('Schlüsselwörter') or '').strip()
            z['Schlüsselwörter'] = f'{kw}, {MARKER}' if kw else MARKER
            z['Beschreibung'] = ''      # Stammzeile trägt den Text, spart 5 MB je Lauf
            neu.append(z)
    return rows + neu, len(neu)


def erweitere_csv(pfad):
    """kahrs_source.csv um die Wunschlängen ergänzen (atomar, idempotent). Gibt Anzahl zurück."""
    with open(pfad, encoding='utf-8-sig', newline='') as f:
        rd = csv.DictReader(f, delimiter=';', quotechar='"')
        felder, rows = rd.fieldnames, list(rd)
    alle, n = ergaenze(rows)
    if not n:
        return 0
    # Kahrs-Zeilen bleiben Byte für Byte gleich, die neuen Zeilen werden nur angehängt
    # (gleiches Format wie Kahrs: ; getrennt, Anführung nur bei Bedarf, Zeilenende \r\n).
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=felder, delimiter=';', quotechar='"', lineterminator='\r\n')
    w.writerows(alle[len(rows):])
    with open(pfad, 'rb') as f:
        alt = f.read()
    if not alt.endswith(b'\n'):
        alt += b'\r\n'
    tmp = pfad + '.wl.tmp'
    with open(tmp, 'wb') as f:
        f.write(alt + buf.getvalue().encode('utf-8'))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, pfad)          # atomar, siehe test_csv_atomar (Wächter las 28.10. halbe CSV)
    return n


if __name__ == '__main__':
    import sys
    p = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                         'kahrs_source.csv')
    print(f'{erweitere_csv(p)} Wunschlängen-Zeilen ergänzt in {p}')
