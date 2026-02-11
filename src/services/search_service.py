# Importar cache service optimizado
from .cache_service import cache_service
from ..utils.Logger import Logger

# Fallback imports para compatibilidad
from ..models.dependencias  import Dependencias
from ..models.programas     import Programas
from ..models.subprogramas  import Subprogramas
from ..models.componentes   import Componentes
from ..models.sexos                     import Sexos
from ..models.estados                   import Estados
from ..models.municipios                import Municipios
from ..models.colonias                  import Colonias
from ..models.estados_civiles           import EstadosCiviles
from ..models.acciones                  import Acciones
from ..models.tipos_beneficios          import TiposBeneficiarios
from ..models.carpeta_beneficiarios     import CarpetaBeneficiarios
from ..models.beneficiarios import Beneficiarios
from ..database.connection  import db

class SearchService:
    """
    Servicio optimizado de búsqueda usando cache inteligente.
    Elimina queries repetidas y mejora performance dramáticamente.
    """

    @staticmethod
    def get_sexo_map():
        """Obtiene mapa de sexos desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('sexos', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para sexos: {str(ex)}")
            # Fallback a query directa
            sexos = (
                Sexos.query
                .with_entities(Sexos.nombre, Sexos.id)
                .filter(Sexos.deleted == 0)
                .all()
            )
            return {nombre.upper().strip(): id_sex for nombre, id_sex in sexos}

    @staticmethod
    def get_estado_map():
        """Obtiene mapa de estados desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('estados', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para estados: {str(ex)}")
            estados = (
                Estados.query
                .with_entities(Estados.nombre, Estados.id)
                .filter(Estados.deleted == 0)
                .all()
            )
            return {nombre.upper().strip(): id_est for nombre, id_est in estados}

    @staticmethod
    def get_municipio_map():
        """Obtiene mapa de municipios desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('municipios', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para municipios: {str(ex)}")
            municipios = (
                Municipios.query
                # Se quito el idEstado por el tema que no esta relacion Municipio con el Estado
                # .with_entities(Municipios.nombre, Municipios.id, Municipios.idEstado)
                .with_entities(Municipios.nombre, Municipios.id)
                .filter(Municipios.deleted == 0)
                .all()
            )
            return {nombre.upper().strip(): [id_mun, id_est] for nombre, id_mun, id_est in municipios}

    @staticmethod
    def get_colonia_map():
        """Obtiene mapa de colonias desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('colonias', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para colonias: {str(ex)}")
            colonias = (
                Colonias.query
                .with_entities(Colonias.nombre, Colonias.id, Colonias.idMunicipio)
                .filter(Colonias.deleted == 0)
                .all()
            )
            return {nombre.upper().strip(): [id_col, id_mun] for nombre, id_col, id_mun in colonias}

    @staticmethod
    def get_estado_civil_map():
        """Obtiene mapa de estados civiles desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('estados_civiles', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para estados civiles: {str(ex)}")
            estados_civiles = (
                EstadosCiviles.query
                .with_entities(EstadosCiviles.nombre, EstadosCiviles.id)
                .filter(EstadosCiviles.deleted == 0)
                .all()
            )
            return {nombre.upper().strip(): id_est_civ for nombre, id_est_civ in estados_civiles}

    @staticmethod
    def get_dependencias_map():
        """Obtiene mapa de dependencias desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('dependencias', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para dependencias: {str(ex)}")
            dependencias = (
                Dependencias.query
                .with_entities(Dependencias.nombre, Dependencias.id)
                .filter(Dependencias.deleted == 0)
                .all()
            )
            return {nombre.upper().strip(): id_dep for nombre, id_dep in dependencias}

    @staticmethod
    def get_programas_map():
        """Obtiene mapa de programas desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('programas', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para programas: {str(ex)}")
            programas = (
                Programas.query
                .with_entities(Programas.nombre, Programas.id, Programas.idDependencia)
                .filter(Programas.deleted == 0)
                .all()
            )
            return {(nombre.upper().strip(), id_dep): id_prog for nombre, id_prog, id_dep in programas}

    @staticmethod
    def get_subprogramas_map():
        """Obtiene mapa de subprogramas desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('subprogramas', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para subprogramas: {str(ex)}")
            subprogramas = (
                Subprogramas.query
                .with_entities(Subprogramas.nombre, Subprogramas.id, Subprogramas.idPrograma)
                .filter(Subprogramas.deleted == 0)
                .all()
            )
            return {(nombre.upper().strip(), id_prog): id_sub for nombre, id_sub, id_prog in subprogramas}

    @staticmethod
    def get_componentes_map():
        """Obtiene mapa de componentes desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('componentes', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para componentes: {str(ex)}")
            componentes = (
                Componentes.query
                .with_entities(Componentes.nombre, Componentes.id, Componentes.idSubPrograma)
                .filter(Componentes.deleted == 0)
                .all()
            )
            return {(nombre.upper().strip(), id_sub): id_com for nombre, id_com, id_sub in componentes}

#     @staticmethod
#     def get_beneficiarios_map():
#         """
#         OPTIMIZADO: Usa cache de beneficiarios con índices O(1).
# 
#         IMPORTANTE: Para compatibilidad con código existente, devuelve el
#         formato original pero con performance mejorada.
#         """
#         try:
#             beneficiarios_cache = cache_service.get_beneficiarios_cache()
#             # Convertir índice combinado al formato esperado por el código existente
#             return beneficiarios_cache.get('combined', {})
#         except Exception as ex:
#             Logger.add_to_log("warning", f"Cache fallback para beneficiarios: {str(ex)}")
#             beneficiarios = (
#                 Beneficiarios.query
#                 .with_entities(Beneficiarios.CURP, Beneficiarios.RFC, Beneficiarios.id)
#                 .filter(Beneficiarios.deleted == 0)
#                 .all()
#             )
#             return {(curp, rfc): id_ben for curp, rfc, id_ben in beneficiarios}

    @staticmethod
    def get_acciones_map():
        """Obtiene mapa de acciones desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('acciones', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para acciones: {str(ex)}")
            tipos_acciones = (
                Acciones.query
                .with_entities(Acciones.nombre, Acciones.id)
                .filter(Acciones.deleted == 0)
                .all()
            )
            return {nombre.upper().strip(): id_act for nombre, id_act in tipos_acciones}

    @staticmethod
    def get_tipos_beneficiarios_map():
        """Obtiene mapa de tipos beneficiarios desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('tipos_beneficiarios', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para tipos beneficiarios: {str(ex)}")
            tipos_beneficiarios = (
                TiposBeneficiarios.query
                .with_entities(TiposBeneficiarios.nombre, TiposBeneficiarios.id)
                .filter(TiposBeneficiarios.deleted == 0)
                .all()
            )
            return {nombre.upper().strip(): id_tben for nombre, id_tben in tipos_beneficiarios}

    @staticmethod
    def get_carpeta_beneficiarios_map():
        """Obtiene mapa de carpetas beneficiarios desde cache optimizado."""
        try:
            catalogs = cache_service.get_catalogs()
            return catalogs.get('carpetas_beneficiarios', {})
        except Exception as ex:
            Logger.add_to_log("warning", f"Cache fallback para carpetas beneficiarios: {str(ex)}")
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
            return {
                (mes, anio, id_dep): {
                    "id": id_carpeta,
                    "estado": estado
                }
                for id_carpeta, mes, anio, id_dep, estado in carpetas_beneficiarios
            }

    @staticmethod
    def find_beneficiario_optimized(curp=None, rfc=None):
        """
        NUEVO: Búsqueda optimizada O(1) de beneficiarios usando cache avanzado.

        Args:
            curp: CURP del beneficiario
            rfc: RFC del beneficiario

        Returns:
            ID del beneficiario si existe, None en caso contrario
        """
        return cache_service.find_beneficiario(curp, rfc)

    @staticmethod
    def get_cache_stats():
        """Obtiene estadísticas del cache para monitoring."""
        return cache_service.get_cache_stats()

    @staticmethod
    def force_refresh_cache():
        """Fuerza refresco del cache (útil para testing o actualizaciones manuales)."""
        cache_service.force_refresh()

    @staticmethod
    def individual_refresh_cache(type_catalog):
        
        actions = {
            '1': cache_service.refresh_dependencias_cache,
            '2': cache_service.refresh_programas_cache,
            '3': cache_service.refresh_subprogramas_cache,
            '4': cache_service.refresh_componentes_cache,
            '5': cache_service.refresh_acciones_cache,
            '6': cache_service.refresh_estados_cache,
            '7': cache_service.refresh_municipios_cache,
            '8': cache_service.refresh_colonias_cache,
            '9': cache_service.refresh_sexos_cache,
            '10': cache_service.refresh_estados_civiles_cache,
            '11': cache_service.refresh_tipos_beneficiarios_cache,
            '12': cache_service.refresh_carpetas_beneficiarios_cache,
            '13': cache_service.refresh_beneficiarios_cache,
        }

        if type_catalog not in actions:
            raise ValueError(f"El tipo de catálogo '{type_catalog}' no existe.")

        # Ejecutar la acción correspondiente
        actions[type_catalog]()