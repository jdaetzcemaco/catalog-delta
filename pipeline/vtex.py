"""
VTEX read-only helpers. The public catalog search returns, per SKU, the sellable
quantity (`AvailableQuantity`) and prices without touching per-SKU logistics APIs.
"""

from __future__ import annotations

from dataclasses import dataclass

import requests


@dataclass
class VTEXClient:
    account: str
    app_key: str = ""
    app_token: str = ""
    environment: str = "vtexcommercestable"

    @property
    def base(self) -> str:
        return f"https://{self.account}.{self.environment}.com.br"

    def _headers(self) -> dict:
        h = {"Accept": "application/json"}
        if self.app_key and self.app_token:
            h.update({"X-VTEX-API-AppKey": self.app_key, "X-VTEX-API-AppToken": self.app_token})
        return h

    def sku_offer(self, sku: str) -> dict | None:
        """{'sku', 'product_id', 'available', 'price', 'list_price', 'sellers'} or None when unknown."""
        r = requests.get(f"{self.base}/api/catalog_system/pub/products/search", params={"fq": f"skuId:{sku}"},
                         headers=self._headers(), timeout=30)
        if r.status_code == 404 or not r.text.strip():
            return None
        r.raise_for_status()
        for product in r.json():
            for item in product.get("items", []):
                if str(item.get("itemId")) == str(sku):
                    offers = [s.get("commertialOffer", {}) for s in item.get("sellers", [])]
                    return {
                        "sku": str(sku), "product_id": str(product.get("productId")),
                        "available": sum(int(o.get("AvailableQuantity") or 0) for o in offers),
                        "price": max((o.get("Price") or 0) for o in offers) if offers else None,
                        "list_price": max((o.get("ListPrice") or 0) for o in offers) if offers else None,
                        "sellers": len(offers),
                    }
        return None
