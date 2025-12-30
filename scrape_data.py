from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

BASE = "https://web-scraping.dev"
OUT_FILE = "data.json"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    }
)

PRODUCT_ID_RE = re.compile(r"(?:https?://web-scraping\.dev)?/product/(\d+)", re.IGNORECASE)
PRICE_RE = re.compile(r"\b(\d{1,5}\.\d{2})\b")


def get_soup(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Tuple[BeautifulSoup, str]:
    h = dict(SESSION.headers)
    if headers:
        h.update(headers)
    r = SESSION.get(url, params=params, headers=h, timeout=25)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser"), r.text


def _extract_price(text: str) -> Optional[str]:
    m = PRICE_RE.findall(text or "")
    return m[-1] if m else None


def scrape_products(max_pages: int = 200, sleep_s: float = 0.2) -> List[Dict[str, Any]]:
    categories = ["apparel", "consumables"]
    all_products: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    for cat in categories:
        for page in range(1, max_pages + 1):
            soup, _ = get_soup(urljoin(BASE, "/products"), params={"category": cat, "page": page})
            anchors = soup.find_all("a", href=True)

            page_items = 0
            for a in anchors:
                href = (a.get("href") or "").strip()
                m = PRODUCT_ID_RE.search(href)
                if not m:
                    continue

                pid = m.group(1)
                if pid in seen_ids:
                    continue

                name = a.get_text(" ", strip=True)
                if not name:
                    continue

                price = None
                node = a
                for _ in range(6):
                    node = getattr(node, "parent", None)
                    if node is None:
                        break
                    txt = node.get_text(" ", strip=True)
                    price = _extract_price(txt)
                    if price:
                        break

                all_products.append(
                    {"id": pid, "name": name, "price": price, "url": urljoin(BASE, href), "category": cat}
                )
                seen_ids.add(pid)
                page_items += 1

            print(f"[products:{cat}] page={page} -> {page_items} new (total={len(all_products)})")
            if page_items == 0:
                break

            time.sleep(sleep_s)

    return all_products


def dedupe_products_by_name_price(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for p in products:
        name = (p.get("name") or "").strip()
        price = (p.get("price") or "").strip()
        key = (name.lower(), price)
        if key in seen:
            continue
        seen.add(key)
        out.append({"id": p.get("id"), "name": name, "price": price, "url": p.get("url")})
    return out


def scrape_testimonials(max_pages: int = 200, sleep_s: float = 0.15) -> List[Dict[str, Any]]:
    api_url = urljoin(BASE, "/api/testimonials")
    referer = urljoin(BASE, "/testimonials")

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for page in range(1, max_pages + 1):
        r = SESSION.get(api_url, params={"page": page}, headers={"Referer": referer}, timeout=25)
        if r.status_code in (401, 403):
            r = SESSION.get(
                api_url,
                params={"page": page},
                headers={"Referer": referer, "X-Secret-Token": "secret123"},
                timeout=25,
            )
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        main = soup.select_one("main") or soup

        candidates: List[str] = []
        for el in main.select("p, blockquote, li, .testimonial, .testimonial-text"):
            t = el.get_text(" ", strip=True)
            if t and len(t) >= 20:
                candidates.append(t)

        added = 0
        for t in candidates:
            if t in seen:
                continue
            seen.add(t)
            out.append({"comment": t})
            added += 1

        print(f"[testimonials api] page={page} -> +{added} (total={len(out)})")
        if added == 0:
            break

        time.sleep(sleep_s)

    return out


def _parse_date(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            ts = float(val)
            if ts > 10_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None

    s = str(val).strip()
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _keep_only_2023(dt: Optional[datetime]) -> Optional[datetime]:
    # IMPORTANT FIX: do NOT overwrite year
    if dt is None:
        return None
    return dt if dt.year == 2023 else None


def try_fetch_reviews_api(max_pages: int = 200, sleep_s: float = 0.15) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    out: List[Dict[str, Any]] = []
    headers = {"x-csrf-token": "secret-csrf-token-123"}

    for page in range(1, max_pages + 1):
        r = SESSION.get(urljoin(BASE, "/api/reviews"), headers=headers, params={"page": page}, timeout=25)
        if r.status_code != 200:
            return [], r.status_code

        try:
            data = r.json()
        except Exception:
            return [], 0

        items = None
        if isinstance(data, dict):
            for k in ("reviews", "items", "results", "data"):
                v = data.get(k)
                if isinstance(v, list):
                    items = v
                    break
        elif isinstance(data, list):
            items = data

        if not items:
            break

        added = 0
        for it in items:
            if not isinstance(it, dict):
                continue

            text = (it.get("text") or it.get("body") or it.get("comment") or it.get("review") or "").strip()
            if not text:
                continue

            dt = _keep_only_2023(
                _parse_date(it.get("date") or it.get("created_at") or it.get("createdAt") or it.get("timestamp"))
            )
            if dt is None:
                continue

            out.append(
                {
                    "product_id": it.get("product_id") or it.get("productId"),
                    "date": dt.date().isoformat(),
                    "text": text,
                    "rating": it.get("rating") or it.get("stars") or it.get("score"),
                    "author": it.get("author") or it.get("user") or it.get("name"),
                    "source": "api",
                }
            )
            added += 1

        print(f"[reviews api] page={page} -> +{added} (total={len(out)})")
        time.sleep(sleep_s)

    return out, None


def extract_json_blobs(html: str) -> List[Any]:
    blobs: List[Any] = []
    decoder = json.JSONDecoder()
    i = 0
    while i < len(html):
        if html[i] in "{[":
            try:
                obj, end = decoder.raw_decode(html[i:])
                blobs.append(obj)
                i += end
                continue
            except Exception:
                pass
        i += 1
    return blobs


def _normalize_review_obj(r: Dict[str, Any], product_id: str) -> Optional[Dict[str, Any]]:
    text = (r.get("text") or r.get("body") or r.get("comment") or r.get("review") or "").strip()
    if not text:
        return None

    dt = _keep_only_2023(
        _parse_date(r.get("date") or r.get("created_at") or r.get("createdAt") or r.get("timestamp"))
    )
    if dt is None:
        return None

    return {"product_id": product_id, "date": dt.date().isoformat(), "text": text, "source": "product_page"}


def scrape_reviews_from_product_pages(products_raw: List[Dict[str, Any]], max_products: int = 60, sleep_s: float = 0.15) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for p in products_raw[:max_products]:
        pid = str(p.get("id") or "").strip()
        url = p.get("url")
        if not pid or not url:
            continue

        r = SESSION.get(url, timeout=25)
        r.raise_for_status()

        found = 0
        for b in extract_json_blobs(r.text):
            if isinstance(b, dict):
                for key in ("reviews", "review", "customerReviews"):
                    v = b.get(key)
                    if isinstance(v, list):
                        for rr in v:
                            if isinstance(rr, dict):
                                norm = _normalize_review_obj(rr, pid)
                                if norm:
                                    k = (norm["product_id"], norm["date"], norm["text"])
                                    if k not in seen:
                                        seen.add(k)
                                        out.append(norm)
                                        found += 1

            if isinstance(b, list) and b and isinstance(b[0], dict):
                sample = b[0]
                if any(k in sample for k in ("date", "created_at", "createdAt", "timestamp")) and any(
                    k in sample for k in ("text", "body", "comment", "review")
                ):
                    for rr in b:
                        if isinstance(rr, dict):
                            norm = _normalize_review_obj(rr, pid)
                            if norm:
                                k = (norm["product_id"], norm["date"], norm["text"])
                                if k not in seen:
                                    seen.add(k)
                                    out.append(norm)
                                    found += 1

        print(f"[reviews per product] product_id={pid} -> {found}")
        time.sleep(sleep_s)

    return out


def main() -> None:
    products_raw = scrape_products()
    products = dedupe_products_by_name_price(products_raw)
    testimonials = scrape_testimonials()

    reviews, api_err = try_fetch_reviews_api()
    if not reviews:
        print(f"[reviews] API not usable (status={api_err}). Falling back to product pages...")
        reviews = scrape_reviews_from_product_pages(products_raw)

    payload = {
        "meta": {"source": BASE, "scraped_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")},
        "products": products,
        "testimonials": testimonials,
        "reviews": reviews,
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved -> {OUT_FILE} | reviews={len(reviews)}")


if __name__ == "__main__":
    main()
