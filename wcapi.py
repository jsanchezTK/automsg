import os
import sys
from typing import Any, Dict, List, Optional, Union, TypedDict
import requests


# ----------------------------
# Tipos (opcionales)
# ----------------------------
QueryPrimitive = Union[str, int, float, bool]
QueryValue = Union[QueryPrimitive, List[QueryPrimitive]]


class WooProductQuery(TypedDict, total=False):
    search: str
    category: Union[str, int]
    tag: Union[str, int]
    include: List[int]
    exclude: List[int]
    min_price: Union[str, int, float]
    max_price: Union[str, int, float]
    status: str
    featured: bool
    on_sale: bool
    order: str
    orderby: str
    page: int
    per_page: int


def _serialize_query_value(value: QueryValue) -> str:
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return str(value)


def build_query_params(query: WooProductQuery) -> Dict[str, str]:
    params: Dict[str, str] = {}

    def append(key: str, value: Optional[QueryValue]) -> None:
        if value is None:
            return
        params[key] = _serialize_query_value(value)

    append("search", query.get("search"))
    append("category", query.get("category"))
    append("tag", query.get("tag"))
    append("include", query.get("include"))
    append("exclude", query.get("exclude"))
    append("min_price", query.get("min_price"))
    append("max_price", query.get("max_price"))
    append("status", query.get("status"))
    append("featured", query.get("featured"))
    append("on_sale", query.get("on_sale"))
    append("order", query.get("order"))
    append("orderby", query.get("orderby"))
    append("page", query.get("page"))
    append("per_page", query.get("per_page"))

    return params


def _dump_error(resp: requests.Response, label: str) -> None:
    print(f"\n--- {label} ERROR ---")
    print("URL:", resp.url)
    print("STATUS:", resp.status_code)
    try:
        print("BODY (json):", resp.json())
    except Exception:
        print("BODY (text):", resp.text[:2000])


def listar_excursiones_woo(
    query: Optional[WooProductQuery] = None,
    timeout_seconds: int = 15,
    debug: bool = True,
) -> Dict[str, Any]:
    base_url = "www.turistik.com"
    consumer_key = os.getenv("WC_CLIENT_KEY")
    consumer_secret = os.getenv("WC_CLIENT_SECRET")

    if not base_url or not consumer_key or not consumer_secret:
        raise RuntimeError(
            "Faltan variables de entorno: WC_BASE_URL, WC_CONSUMER_KEY, WC_CONSUMER_SECRET"
        )

    base_url = base_url.rstrip("/") + "/wp-json/wc/v3"
    endpoint = f"{base_url}/products"

    query = query or {}
    params = build_query_params(query)

    s = requests.Session()

    # Evita proxies del entorno (común que rompan Authorization)
    s.trust_env = False

    # User-Agent “tipo axios” (a veces ayuda con WAF)
    s.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "axios/1.6.0",
        }
    )

    # Intento A: Basic Auth (como axios)
    resp = s.get(
        endpoint,
        params=params,
        auth=(consumer_key, consumer_secret),
        timeout=timeout_seconds,
    )

    if resp.ok:
        return {
            "data": resp.json(),
            "pagination": {
                "total": int(resp.headers.get("X-WP-Total", "0") or 0),
                "totalPages": int(resp.headers.get("X-WP-TotalPages", "0") or 0),
            },
        }

    if debug:
        _dump_error(resp, "BasicAuth")

    # Intento B: auth por querystring (fallback)
    if resp.status_code in (401, 403):
        params2 = dict(params)
        params2["consumer_key"] = consumer_key
        params2["consumer_secret"] = consumer_secret

        resp2 = s.get(endpoint, params=params2, timeout=timeout_seconds)

        if resp2.ok:
            return {
                "data": resp2.json(),
                "pagination": {
                    "total": int(resp2.headers.get("X-WP-Total", "0") or 0),
                    "totalPages": int(resp2.headers.get("X-WP-TotalPages", "0") or 0),
                },
            }

        if debug:
            _dump_error(resp2, "QueryStringAuth")

        resp2.raise_for_status()

    resp.raise_for_status()
    raise RuntimeError("Unreachable")


def print_first_products(products: List[Dict[str, Any]], n: int = 5) -> None:
    print(f"\nPrimeros {min(n, len(products))} productos:")
    for i, p in enumerate(products[:n], start=1):
        pid = p.get("id")
        name = p.get("name")
        price = p.get("price")
        status = p.get("status")
        permalink = p.get("permalink")
        print(f"{i}. [{pid}] {name} | price={price} | status={status}")
        if permalink:
            print(f"   {permalink}")


if __name__ == "__main__":
    try:
        result = listar_excursiones_woo(
            {"search": "valle", "per_page": 5, "page": 1}, debug=False
        )
        print("OK pagination:", result["pagination"])
        print("OK items:", len(result["data"]))

        # imprime los 5 (en este caso ya pediste per_page=5)
        print_first_products(result["data"], n=5)

    except Exception as e:
        print("\nFALLÓ:", repr(e))
        sys.exit(1)