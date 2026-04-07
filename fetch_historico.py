#!/usr/bin/env python3
"""
Scraper de precios históricos BCR (Cámara Arbitral de Cereales)
Se ejecuta via GitHub Actions y guarda los datos en historico.json
URL: https://www.cac.bcr.com.ar/es/precios-de-pizarra/consultas
"""
import json
import os
import time
import sys
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from html.parser import HTMLParser

# Productos disponibles en la BCR CAC
PRODUCTS = [
    {"key": "soja",    "id": 13, "name": "Soja"},
    {"key": "maiz",    "id": 3,  "name": "Maíz"},
    {"key": "trigo",   "id": 8,  "name": "Trigo"},
    {"key": "girasol", "id": 9,  "name": "Girasol"},
    {"key": "sorgo",   "id": 6,  "name": "Sorgo"},
]

BCR_URL = "https://www.cac.bcr.com.ar/es/precios-de-pizarra/consultas"

class BCRTableParser(HTMLParser):
    """Parsea la tabla HTML de precios de la BCR."""
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_tbody = False
        self.in_row = False
        self.in_cell = False
        self.cell_count = 0
        self.current_row = []
        self.rows = []
        self.depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "tbody":
            self.in_tbody = True
        elif tag == "tr" and self.in_tbody:
            self.in_row = True
            self.current_row = []
            self.cell_count = 0
        elif tag == "td" and self.in_row:
            self.in_cell = True
            self.depth = 0

    def handle_endtag(self, tag):
        if tag == "tbody":
            self.in_tbody = False
        elif tag == "tr" and self.in_row:
            self.in_row = False
            if len(self.current_row) >= 2:
                self.rows.append(self.current_row[:2])
        elif tag == "td" and self.in_cell:
            self.in_cell = False
            self.cell_count += 1

    def handle_data(self, data):
        if self.in_cell:
            text = data.strip()
            if text:
                if self.cell_count >= len(self.current_row):
                    self.current_row.append(text)
                else:
                    self.current_row[self.cell_count] = text


def fetch_page(product_id, date_start, date_end, page=0):
    """Descarga una página de resultados de la BCR."""
    params = {
        "product": product_id,
        "type": "pizarra",
        "date_start": date_start,
        "date_end": date_end,
        "period": "day",
        "op": "Filtrar",
    }
    if page > 0:
        params["page"] = page

    url = f"{BCR_URL}?{urlencode(params)}"
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; DataBot/1.0)",
        "Accept": "text/html",
    })
    try:
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        return html
    except Exception as e:
        print(f"  Error fetching page {page}: {e}")
        return None


def parse_price(price_str):
    """Convierte '$290.000,00' → 290000.0"""
    clean = price_str.replace("$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(clean)
    except:
        return None


def parse_date(date_str):
    """Convierte 'DD/MM/YYYY' → 'YYYY-MM-DD'"""
    parts = date_str.strip().split("/")
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1]}-{parts[0]}"
    return None


def fetch_product_history(product, years=5):
    """Descarga todo el histórico de un producto."""
    today = datetime.now()
    date_end = today.strftime("%Y-%m-%d")
    date_start = (today - timedelta(days=years * 365)).strftime("%Y-%m-%d")

    print(f"\n  Descargando {product['name']} ({date_start} → {date_end})")
    
    all_data = {}  # fecha → precio (dedup)
    page = 0
    max_pages = 200

    while page < max_pages:
        html = fetch_page(product["id"], date_start, date_end, page)
        if not html:
            break

        parser = BCRTableParser()
        parser.feed(html)
        rows = parser.rows

        if not rows:
            print(f"    Página {page}: sin datos → fin")
            break

        new_count = 0
        for row in rows:
            if len(row) < 2:
                continue
            fecha = row[0].strip()
            precio_str = row[1].strip()
            if not fecha.count("/") == 2:
                continue
            fecha_iso = parse_date(fecha)
            precio = parse_price(precio_str)
            if fecha_iso and precio and precio > 0:
                if fecha_iso not in all_data:
                    all_data[fecha_iso] = precio
                    new_count += 1

        print(f"    Página {page}: {len(rows)} filas, {new_count} nuevas. Total: {len(all_data)}")

        # Verificar si hay página siguiente
        has_next = (
            'pager__item--next' in html or
            '>Siguiente<' in html or
            '">»<' in html or
            '>Next<' in html
        )
        if not has_next:
            print(f"    Última página.")
            break

        page += 1
        time.sleep(0.3)  # Ser amable con el servidor

    # Convertir a lista ordenada
    result = sorted(
        [{"fecha": k, "ars": v} for k, v in all_data.items()],
        key=lambda x: x["fecha"]
    )
    print(f"  Total {product['name']}: {len(result)} registros")
    return result


def main():
    print("=" * 50)
    print("BCR Histórico Scraper")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # Cargar datos existentes si hay
    output_file = "historico.json"
    existing = {}
    if os.path.exists(output_file):
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                existing = json.load(f)
            print(f"\nDatos existentes cargados: {list(existing.keys())}")
        except:
            existing = {}

    result = {
        "updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "fuente": "BCR Cámara Arbitral de Cereales - cac.bcr.com.ar",
        "productos": {}
    }

    for product in PRODUCTS:
        try:
            data = fetch_product_history(product, years=5)
            result["productos"][product["key"]] = data
            time.sleep(1)
        except Exception as e:
            print(f"  ERROR en {product['name']}: {e}")
            # Preservar datos anteriores si existen
            if product["key"] in existing.get("productos", {}):
                result["productos"][product["key"]] = existing["productos"][product["key"]]
                print(f"  Usando datos anteriores para {product['name']}")

    # Guardar
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))

    total = sum(len(v) for v in result["productos"].values())
    print(f"\n✓ Guardado en {output_file}")
    print(f"  Total registros: {total}")
    for k, v in result["productos"].items():
        print(f"  {k}: {len(v)} registros")


if __name__ == "__main__":
    main()
