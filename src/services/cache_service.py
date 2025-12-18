# src/services/cache_service.py

from __future__ import annotations

import time
import threading
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ..database.connection import db
from ..utils.Logger import Logger


class CacheService:
    """
    Cache en memoria (por proceso) SOLO para catálogos.
    ✅ Ya NO existe cache de beneficiarios.
    """

    def __init__(self, ttl_seconds: int = 3600):
        self.ttl_seconds = ttl_seconds
        self._lock = threading.RLock()

        # Cache de catálogos:
        # {
        #   "TIPO_CATALOGO": {
        #        "loaded_at": float,
        #        "rows": [ {..}, {..} ],
        #        "by_id": { "<id>": {...} },
        #        "by_name": { "<nombre_normalizado>": {...} }
        #   },
        #   ...
        # }
        self._catalogs_cache: Dict[str, Dict[str, Any]] = {}
        self._catalogs_loaded_at: Optional[float] = None

    # -------------------------
    # Helpers
    # -------------------------
    def _now(self) -> float:
        return time.time()

    def _is_valid(self, loaded_at: Optional[float]) -> bool:
        if loaded_at is None:
            return False
        return (self._now() - loaded_at) <= self.ttl_seconds

    @staticmethod
    def _norm_name(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().lower()

    # -------------------------
    # DB fetch (ajusta aquí si tu SP/consulta es distinta)
    # -------------------------
    def _fetch_catalog_rows(self, catalog_type: str) -> List[Dict[str, Any]]:
        """
        Obtiene el catálogo desde BD.
        Por defecto intenta usar: CALL sp_ListaPorCatalogo(:tipo)

        Ajusta este método si tus catálogos se obtienen con otro SP/consulta.
        """
        sql = text("CALL sp_ListaPorCatalogo(:tipo)")
        result = db.session.execute(sql, {"tipo": catalog_type})

        rows: List[Dict[str, Any]] = []
        # SQLAlchemy RowMapping -> dict
        for r in result.mappings():
            rows.append(dict(r))
        return rows

    def _index_catalog(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Construye índices para búsqueda rápida por id / nombre.
        Detecta columnas comunes: id/Id/ID y nombre/Nombre/descripcion/Descripcion.
        """
        by_id: Dict[str, Dict[str, Any]] = {}
        by_name: Dict[str, Dict[str, Any]] = {}

        # posibles llaves
        id_keys = ("id", "Id", "ID")
        name_keys = ("nombre", "Nombre", "descripcion", "Descripcion", "descripción", "Descripción")

        for row in rows:
            # id
            _id = None
            for k in id_keys:
                if k in row and row[k] is not None:
                    _id = str(row[k])
                    break

            # nombre/descripcion
            _name = None
            for k in name_keys:
                if k in row and row[k] is not None:
                    _name = self._norm_name(row[k])
                    break

            if _id:
                by_id[_id] = row
            if _name:
                by_name[_name] = row

        return {"rows": rows, "by_id": by_id, "by_name": by_name}

    # -------------------------
    # Public API
    # -------------------------
    def refresh_catalogs_cache(self, catalog_types: Optional[List[str]] = None) -> None:
        """
        Refresca (reconstruye) el cache de catálogos.
        Si no se pasan tipos, usa los que ya existan en cache; si no hay, no hace nada.
        """
        with self._lock:
            start = time.perf_counter()

            if catalog_types is None:
                catalog_types = list(self._catalogs_cache.keys())

            if not catalog_types:
                Logger.add_to_log("info", "ℹ️ No hay tipos de catálogo definidos para refrescar.")
                self._catalogs_loaded_at = self._now()
                return

            Logger.add_to_log("info", "🔄 Refrescando cache de catálogos...")

            new_cache: Dict[str, Dict[str, Any]] = {}
            for ctype in catalog_types:
                rows = self._fetch_catalog_rows(ctype)
                indexed = self._index_catalog(rows)
                new_cache[ctype] = {
                    "loaded_at": self._now(),
                    **indexed,
                }

            self._catalogs_cache = new_cache
            self._catalogs_loaded_at = self._now()

            elapsed = time.perf_counter() - start
            Logger.add_to_log("info", f"✅ Cache de catálogos refrescado exitosamente en {elapsed:.2f}s")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")

    def get_catalogs_cache(self) -> Dict[str, Dict[str, Any]]:
        """
        Devuelve el cache de catálogos. Si está expirado, lo refresca.
        """
        with self._lock:
            if not self._is_valid(self._catalogs_loaded_at):
                # Si nunca se definieron tipos, aquí no podemos adivinar cuáles son;
                # por eso refresca solo si ya existen tipos en cache.
                self.refresh_catalogs_cache()
            return self._catalogs_cache

    def get_catalog(self, catalog_type: str) -> Dict[str, Any]:
        """
        Obtiene un catálogo en específico. Si no existe o expiró, lo carga.
        """
        with self._lock:
            entry = self._catalogs_cache.get(catalog_type)
            if entry and self._is_valid(entry.get("loaded_at")):
                return entry

            # cargar solo ese tipo
            rows = self._fetch_catalog_rows(catalog_type)
            indexed = self._index_catalog(rows)
            self._catalogs_cache[catalog_type] = {
                "loaded_at": self._now(),
                **indexed,
            }
            self._catalogs_loaded_at = self._now()
            return self._catalogs_cache[catalog_type]

    def find_in_catalog_by_id(self, catalog_type: str, _id: str) -> Optional[Dict[str, Any]]:
        cat = self.get_catalog(catalog_type)
        return cat["by_id"].get(str(_id))

    def find_in_catalog_by_name(self, catalog_type: str, name: str) -> Optional[Dict[str, Any]]:
        cat = self.get_catalog(catalog_type)
        return cat["by_name"].get(self._norm_name(name))

    def force_refresh(self, catalog_types: Optional[List[str]] = None) -> None:
        """
        Forza refresco SOLO de catálogos.
        ✅ Ya NO refresca beneficiarios.
        """
        Logger.add_to_log("info", "🔄 Forzando refresco completo del cache...")
        self.refresh_catalogs_cache(catalog_types=catalog_types)
        Logger.add_to_log("info", "✅ Refresco completo del cache finalizado")

    def clear_catalogs_cache(self) -> None:
        """
        Limpia SOLO catálogos (por si tienes un endpoint admin).
        """
        with self._lock:
            self._catalogs_cache = {}
            self._catalogs_loaded_at = None
            Logger.add_to_log("info", "🧹 Cache de catálogos limpiado.")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Estadísticas del cache.
        ✅ Sin beneficiarios.
        """
        with self._lock:
            stats = {
                "catalogs_types": len(self._catalogs_cache),
                "catalogs_loaded_at": self._catalogs_loaded_at,
                "ttl_seconds": self.ttl_seconds,
            }
            return stats


# Singleton (si tu proyecto lo usa así)
cache_service = CacheService(ttl_seconds=3600)
