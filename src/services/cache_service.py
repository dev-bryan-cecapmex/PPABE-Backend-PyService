from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, Set
import threading

from ..models.dependencias import Dependencias
from ..models.programas import Programas
from ..models.subprogramas import Subprogramas
from ..models.componentes import Componentes
from ..models.sexos import Sexos
from ..models.estados import Estados
from ..models.municipios import Municipios
from ..models.colonias import Colonias
from ..models.estados_civiles import EstadosCiviles
from ..models.acciones import Acciones
from ..models.tipos_beneficios import TiposBeneficiarios
from ..models.carpeta_beneficiarios import CarpetaBeneficiarios
from ..models.beneficiarios import Beneficiarios

from ..database.connection import db
from ..utils.Logger import Logger


class CacheService:
    """
    Servicio de cache inteligente para catálogos y beneficiarios.
    Elimina consultas repetidas y optimiza búsquedas con índices O(1).
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._catalogs_cache: Dict[str, Any] = {}
            self._beneficiarios_cache: Dict[str, Any] = {}
            self._last_refresh: Optional[datetime] = None
            self._cache_ttl = timedelta(hours=1)  # TTL de 1 hora
            self._initialized = True

    def _is_cache_valid(self) -> bool:
        """Verifica si el cache sigue siendo válido basado en TTL."""
        if self._last_refresh is None:
            return False
        return datetime.now() - self._last_refresh < self._cache_ttl

    def refresh_catalogs_cache(self) -> None:
        """Refresca todos los catálogos en una sola operación."""
        Logger.add_to_log("info", "🔄 Refrescando cache de catálogos...")
        start_time = datetime.now()

        try:
            # Sexos
            sexos = (
                Sexos.query
                .with_entities(Sexos.nombre, Sexos.id)
                .filter(Sexos.deleted == 0)
                .all()
            )
            self._catalogs_cache['sexos'] = {nombre.upper().strip(): id_sex for nombre, id_sex in sexos}

            # Estados
            estados = (
                Estados.query
                .with_entities(Estados.nombre, Estados.id)
                .filter(Estados.deleted == 0)
                .all()
            )
            self._catalogs_cache['estados'] = {nombre.upper().strip(): id_est for nombre, id_est in estados}

            # Municipios
            municipios = (
                Municipios.query
                .with_entities(Municipios.nombre, Municipios.id, Municipios.idEstado)
                .filter(Municipios.deleted == 0)
                .all()
            )
            self._catalogs_cache['municipios'] = {nombre.upper().strip(): [id_mun, id_est] for nombre, id_mun, id_est in municipios}

            # Colonias
            colonias = (
                Colonias.query
                .with_entities(Colonias.nombre, Colonias.id, Colonias.idMunicipio)
                .filter(Colonias.deleted == 0)
                .all()
            )
            # self._catalogs_cache['colonias'] = {nombre.upper().strip(): [id_col, id_mun] for nombre, id_col, id_mun in colonias}

            # Estados Civiles
            estados_civiles = (
                EstadosCiviles.query
                .with_entities(EstadosCiviles.nombre, EstadosCiviles.id)
                .filter(EstadosCiviles.deleted == 0)
                .all()
            )
            self._catalogs_cache['estados_civiles'] = {nombre.upper().strip(): id_est_civ for nombre, id_est_civ in estados_civiles}

            # Dependencias
            dependencias = (
                Dependencias.query
                .with_entities(Dependencias.nombre, Dependencias.id)
                .filter(Dependencias.deleted == 0)
                .all()
            )
            self._catalogs_cache['dependencias'] = {nombre.upper().strip(): id_dep for nombre, id_dep in dependencias}

            # Programas
            programas = (
                Programas.query
                .with_entities(Programas.nombre, Programas.id, Programas.idDependencia)
                .filter(Programas.deleted == 0)
                .all()
            )
            self._catalogs_cache['programas'] = {(nombre.upper().strip(), id_dep): id_prog for nombre, id_prog, id_dep in programas}

            # Subprogramas
            subprogramas = (
                Subprogramas.query
                .with_entities(Subprogramas.nombre, Subprogramas.id, Subprogramas.idPrograma)
                .filter(Subprogramas.deleted == 0)
                .all()
            )
            self._catalogs_cache['subprogramas'] = {(nombre.upper().strip(), id_prog): id_sub for nombre, id_sub, id_prog in subprogramas}

            # Componentes
            componentes = (
                Componentes.query
                .with_entities(Componentes.nombre, Componentes.id, Componentes.idSubPrograma)
                .filter(Componentes.deleted == 0)
                .all()
            )
            self._catalogs_cache['componentes'] = {(nombre.upper().strip(), id_sub): id_com for nombre, id_com, id_sub in componentes}

            # Acciones
            acciones = (
                Acciones.query
                .with_entities(Acciones.nombre, Acciones.id)
                .filter(Acciones.deleted == 0)
                .all()
            )
            self._catalogs_cache['acciones'] = {nombre.upper().strip(): id_act for nombre, id_act in acciones}

            # Tipos Beneficiarios
            tipos_beneficiarios = (
                TiposBeneficiarios.query
                .with_entities(TiposBeneficiarios.nombre, TiposBeneficiarios.id)
                .filter(TiposBeneficiarios.deleted == 0)
                .all()
            )
            self._catalogs_cache['tipos_beneficiarios'] = {nombre.upper().strip(): id_tben for nombre, id_tben in tipos_beneficiarios}

            # Carpetas Beneficiarios
            carpetas_beneficiarios = (
                CarpetaBeneficiarios.query
                .with_entities(
                    CarpetaBeneficiarios.id,
                    CarpetaBeneficiarios.mes,
                    CarpetaBeneficiarios.anio,
                    CarpetaBeneficiarios.idDependencia,
                    CarpetaBeneficiarios.estado,
                )
                .filter(CarpetaBeneficiarios.deleted == 0)
                .all()
            )
            self._catalogs_cache['carpetas_beneficiarios'] = {
                (mes, anio, id_dep): {
                    "id": id_carpeta,
                    "estado": estado
                }
                for id_carpeta, mes, anio, id_dep, estado in carpetas_beneficiarios
            }

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()

            Logger.add_to_log("info", f"✅ Cache de catálogos refrescado exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")

        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de catálogos: {str(ex)}")
            raise

#     def refresh_beneficiarios_cache(self) -> None:
#         """Refresca cache de beneficiarios con índices optimizados."""
#         Logger.add_to_log("info", "🔄 Refrescando cache de beneficiarios...")
#         start_time = datetime.now()
# 
#         try:
#             # Cargar todos los beneficiarios activos
#             beneficiarios = (
#                 Beneficiarios.query
#                 .with_entities(Beneficiarios.CURP, Beneficiarios.RFC, Beneficiarios.id)
#                 .filter(Beneficiarios.deleted == 0)
#                 .all()
#             )
# 
#             # Crear múltiples índices para búsquedas O(1)
#             combined_index = {}  # (curp, rfc) -> id
#             curp_index = {}      # curp -> set(ids)
#             rfc_index = {}       # rfc -> set(ids)
# 
#             for curp, rfc, id_ben in beneficiarios:
#                 # Normalizar valores
#                 curp_clean = curp.strip() if curp else None
#                 rfc_clean = rfc.strip() if rfc else None
# 
#                 # Índice combinado
#                 if curp_clean and rfc_clean:
#                     combined_index[(curp_clean, rfc_clean)] = id_ben
# 
#                 # Índice por CURP
#                 if curp_clean:
#                     if curp_clean not in curp_index:
#                         curp_index[curp_clean] = set()
#                     curp_index[curp_clean].add(id_ben)
# 
#                 # Índice por RFC
#                 if rfc_clean:
#                     if rfc_clean not in rfc_index:
#                         rfc_index[rfc_clean] = set()
#                     rfc_index[rfc_clean].add(id_ben)
# 
#             self._beneficiarios_cache = {
#                 'combined': combined_index,
#                 'curp': curp_index,
#                 'rfc': rfc_index,
#                 'total_count': len(beneficiarios)
#             }
# 
#             elapsed_time = (datetime.now() - start_time).total_seconds()
#             Logger.add_to_log("info", f"✅ Cache de beneficiarios refrescado en {elapsed_time:.2f}s")
#             Logger.add_to_log("info", f"   📊 Beneficiarios indexados: {len(beneficiarios)}")
# 
#         except Exception as ex:
#             Logger.add_to_log("error", f"❌ Error refrescando cache de beneficiarios: {str(ex)}")
#             raise

    def get_catalogs(self) -> Dict[str, Any]:
        """Obtiene todos los catálogos, refrescando cache si es necesario."""
        if not self._is_cache_valid() or not self._catalogs_cache:
            self.refresh_catalogs_cache()
        return self._catalogs_cache

    def get_beneficiarios_cache(self) -> Dict[str, Any]:
        """Obtiene cache de beneficiarios, refrescando si es necesario."""
        if not self._beneficiarios_cache:
            self.refresh_beneficiarios_cache()
        return self._beneficiarios_cache

    def find_beneficiario(self, curp: Optional[str] = None, rfc: Optional[str] = None) -> Optional[str]:
        """
        Búsqueda optimizada O(1) de beneficiario por CURP y/o RFC.

        Args:
            curp: CURP del beneficiario
            rfc: RFC del beneficiario

        Returns:
            ID del beneficiario si existe, None en caso contrario
        """
        if not curp and not rfc:
            return None

        cache = self.get_beneficiarios_cache()

        # Normalizar entradas
        curp_clean = curp.strip() if curp else None
        rfc_clean = rfc.strip() if rfc else None

        # Búsqueda por combinación (más específica)
        if curp_clean and rfc_clean:
            return cache['combined'].get((curp_clean, rfc_clean))

        # Búsqueda solo por CURP
        elif curp_clean:
            curp_matches = cache['curp'].get(curp_clean, set())
            return next(iter(curp_matches)) if curp_matches else None

        # Búsqueda solo por RFC
        elif rfc_clean:
            rfc_matches = cache['rfc'].get(rfc_clean, set())
            return next(iter(rfc_matches)) if rfc_matches else None

        return None

    def get_cache_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del cache para monitoring."""
        return {
            'catalogs_loaded': len(self._catalogs_cache),
            'beneficiarios_indexed': self._beneficiarios_cache.get('total_count', 0),
            'last_refresh': self._last_refresh.isoformat() if self._last_refresh else None,
            'cache_valid': self._is_cache_valid(),
            'ttl_remaining_minutes': (self._cache_ttl - (datetime.now() - self._last_refresh)).total_seconds() / 60 if self._last_refresh else 0
        }

    def force_refresh(self) -> None:
        """Fuerza refresco completo del cache."""
        Logger.add_to_log("info", "🔄 Forzando refresco completo del cache...")
        self.refresh_catalogs_cache()
        self.refresh_beneficiarios_cache()
        Logger.add_to_log("info", "✅ Refresco completo del cache finalizado")
    
    def refresh_dependencias_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de dependencias...")
            start_time = datetime.now()
            
            # Dependencias
            dependencias = (
                Dependencias.query
                .with_entities(Dependencias.nombre, Dependencias.id)
                .filter(Dependencias.deleted == 0)
                .all()
            )
            self._catalogs_cache['dependencias'] = {nombre.upper().strip(): id_dep for nombre, id_dep in dependencias}
            
            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de Dependencias refrescado exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de Dependencias: {str(ex)}")
            raise
    
    def refresh_programas_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de programas...")
            start_time = datetime.now()
            
            # Programas
            programas = (
                Programas.query
                .with_entities(Programas.nombre, Programas.id, Programas.idDependencia)
                .filter(Programas.deleted == 0)
                .all()
            )
            self._catalogs_cache['programas'] = {(nombre.upper().strip(), id_dep): id_prog for nombre, id_prog, id_dep in programas}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de Programas refrescado exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de Programas: {str(ex)}")
            raise
    
    def refresh_subprogramas_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de subprogramas...")
            start_time = datetime.now()
            
            # Subprogramas
            subprogramas = (
                Subprogramas.query
                .with_entities(Subprogramas.nombre, Subprogramas.id, Subprogramas.idPrograma)
                .filter(Subprogramas.deleted == 0)
                .all()
            )
            self._catalogs_cache['subprogramas'] = {(nombre.upper().strip(), id_prog): id_sub for nombre, id_sub, id_prog in subprogramas}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de subprogramas refrescado exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de Subprogramas: {str(ex)}")
            raise
        
        
    def refresh_componentes_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de componentes...")
            start_time = datetime.now()
            
            # Componentes
            componentes = (
                Componentes.query
                .with_entities(Componentes.nombre, Componentes.id, Componentes.idSubPrograma)
                .filter(Componentes.deleted == 0)
                .all()
            )
            self._catalogs_cache['componentes'] = {(nombre.upper().strip(), id_sub): id_com for nombre, id_com, id_sub in componentes}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de componentes refrescado exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de Componentes: {str(ex)}")
            raise
        
    def refresh_acciones_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de acciones...")
            start_time = datetime.now()
            
            # Acciones
            acciones = (
                Acciones.query
                .with_entities(Acciones.nombre, Acciones.id)
                .filter(Acciones.deleted == 0)
                .all()
            )
            self._catalogs_cache['acciones'] = {nombre.upper().strip(): id_act for nombre, id_act in acciones}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de acciones refrescado exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de Acciones: {str(ex)}")
            raise
    
    def refresh_estados_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de estados...")
            start_time = datetime.now()
            
            # Estados
            estados = (
                Estados.query
                .with_entities(Estados.nombre, Estados.id)
                .filter(Estados.deleted == 0)
                .all()
            )
            self._catalogs_cache['estados'] = {nombre.upper().strip(): id_est for nombre, id_est in estados}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de estados refrescado exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de Estados: {str(ex)}")
            raise
    
    def refresh_municipios_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de municipios...")
            start_time = datetime.now()
            
            # Municipios
            municipios = (
                Municipios.query
                .with_entities(Municipios.nombre, Municipios.id, Municipios.idEstado)
                .filter(Municipios.deleted == 0)
                .all()
            )
            self._catalogs_cache['municipios'] = {nombre.upper().strip(): [id_mun, id_est] for nombre, id_mun, id_est in municipios}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de municipios refrescado exitosamente en {elapsed_time:.2f}s")
            # Logger.add_to_log("info", f"Datos: {self._catalogs_cache}")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de Municipios: {str(ex)}")
            raise
        
    def refresh_colonias_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de colonias...")
            start_time = datetime.now()
            
            # Colonias
            colonias = (
                Colonias.query
                .with_entities(Colonias.nombre, Colonias.id, Colonias.idMunicipio)
                .filter(Colonias.deleted == 0)
                .all()
            )
            self._catalogs_cache['colonias'] = {nombre.upper().strip(): [id_col, id_mun] for nombre, id_col, id_mun in colonias}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de colonias refrescado exitosamente en {elapsed_time:.2f}s")
            # Logger.add_to_log("info", f"Datos: {self._catalogs_cache}")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de Colonias: {str(ex)}")
            raise
    
    def refresh_sexos_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de sexos...")
            start_time = datetime.now()
            
            # Sexos
            sexos = (
                Sexos.query
                .with_entities(Sexos.nombre, Sexos.id)
                .filter(Sexos.deleted == 0)
                .all()
            )
            self._catalogs_cache['sexos'] = {nombre.upper().strip(): id_sex for nombre, id_sex in sexos}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de sexos refrescado exitosamente en {elapsed_time:.2f}s")
            #Logger.add_to_log("info", f"Datos: {self._catalogs_cache}")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de sexos: {str(ex)}")
            raise

    def refresh_estados_civiles_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de estados civiles...")
            start_time = datetime.now()
            
            # Estados Civiles
            estados_civiles = (
                EstadosCiviles.query
                .with_entities(EstadosCiviles.nombre, EstadosCiviles.id)
                .filter(EstadosCiviles.deleted == 0)
                .all()
            )
            self._catalogs_cache['estados_civiles'] = {nombre.upper().strip(): id_est_civ for nombre, id_est_civ in estados_civiles}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de estados civiles refrescado exitosamente en {elapsed_time:.2f}s")
            # Logger.add_to_log("info", f"Datos: {self._catalogs_cache}")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de estados civiles: {str(ex)}")
            raise
        
    def refresh_tipos_beneficiarios_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de tipo beneficiario...")
            start_time = datetime.now()
            
            # Tipos Beneficiarios
            tipos_beneficiarios = (
                TiposBeneficiarios.query
                .with_entities(TiposBeneficiarios.nombre, TiposBeneficiarios.id)
                .filter(TiposBeneficiarios.deleted == 0)
                .all()
            )
            self._catalogs_cache['tipos_beneficiarios'] = {nombre.upper().strip(): id_tben for nombre, id_tben in tipos_beneficiarios}

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de tipo beneficiario refrescados exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"Datos: {self._catalogs_cache}")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de tipo beneficiarios: {str(ex)}")
            raise
    
    def refresh_carpetas_beneficiarios_cache(self) -> None:
        try:
            Logger.add_to_log("info", "🔄 Refrescando cache de carpetas beneficiarios...")
            start_time = datetime.now()
            
            # Carpetas Beneficiarios
            carpetas_beneficiarios = (
                CarpetaBeneficiarios.query
                .with_entities(
                    CarpetaBeneficiarios.id,
                    CarpetaBeneficiarios.mes,
                    CarpetaBeneficiarios.anio,
                    CarpetaBeneficiarios.idDependencia,
                    CarpetaBeneficiarios.estado,
                )
                .filter(CarpetaBeneficiarios.deleted == 0)
                .all()
            )
            self._catalogs_cache['carpetas_beneficiarios'] = {
                (mes, anio, id_dep): {
                    "id": id_carpeta,
                    "estado": estado
                }
                for id_carpeta, mes, anio, id_dep, estado in carpetas_beneficiarios
            }

            self._last_refresh = datetime.now()
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            Logger.add_to_log("info", f"✅ Cache de carpetas beneficiario refrescados exitosamente en {elapsed_time:.2f}s")
            Logger.add_to_log("info", f"Datos: {self._catalogs_cache}")
            Logger.add_to_log("info", f"   📊 Catálogos cargados: {len(self._catalogs_cache)} tipos")
        except Exception as ex:
            Logger.add_to_log("error", f"❌ Error refrescando cache de carpetas beneficiarios: {str(ex)}")
            raise
        
# Instancia singleton global
cache_service = CacheService()