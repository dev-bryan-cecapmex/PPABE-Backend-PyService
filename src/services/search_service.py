# Importar los modelos para buscar su informacion
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
    
    @staticmethod
    def get_sexo_map():
        sexos = (
            Sexos.query
            .with_entities( Sexos.nombre, Sexos.id)
            .filter(Sexos.deleted == 0)
            .all()
        )
        return {nombre: id_sex for nombre, id_sex in sexos}
    
    @staticmethod
    def get_estado_map():
        estados = (
            Estados.query
            .with_entities( Estados.nombre, Estados.id )
            .filter(Estados.deleted == 0)
            .all()
        )
        return {nombre:id_est for nombre, id_est in estados}
    
    @staticmethod
    def get_municipio_map():
        municipios = (
            Municipios.query
            .with_entities(Municipios.nombre, Municipios.idEstado, Municipios.id)
            .filter(Municipios.deleted == 0)
            .all()
        )
        return {nombre:[id_mun,id_est] for nombre, id_mun, id_est in municipios}
        
    @staticmethod
    def get_colonia_map():
        colonias = (
            Colonias.query
            .with_entities(Colonias.nombre, Colonias.id, Colonias.idMunicipio)
            .filter(Colonias.deleted == 0)
            .all()
        )
        return {nombre:[id_col, id_mun] for nombre, id_col, id_mun in colonias}
    
    @staticmethod
    def get_estado_civil_map():
        estados_civiles = (
            EstadosCiviles.query
            .with_entities( EstadosCiviles.nombre, EstadosCiviles.id)
            .filter(EstadosCiviles.deleted == 0)
            .all()
        )
        return {nombre: id_est_civ for nombre, id_est_civ in estados_civiles}
    
    @staticmethod
    def get_dependencias_map():
        dependencias = (
            Dependencias.query
            .with_entities( Dependencias.nombre, Dependencias.id)
            .filter(Dependencias.deleted == 0)
            .all()
        )
        return {nombre: id_dep for nombre, id_dep in dependencias}
    
    @staticmethod
    def get_programas_map():
        programas = (
            Programas.query
            .with_entities(Programas.nombre, Programas.id, Programas.idDependencia)
            .filter(Programas.deleted == 0)
            .all()
        )
        #return {(nombre, id_prog): id_dep for nombre, id_prog, id_dep in programas}
        return {(nombre, id_dep): id_prog for nombre, id_prog, id_dep in programas}
    
    @staticmethod
    def get_subprogramas_map():
        subprogramas = (
            Subprogramas.query
            .with_entities(Subprogramas.nombre, Subprogramas.id, Subprogramas.idPrograma)
            .filter(Subprogramas.deleted == 0)
            .all()
        )
        return {(nombre, id_prog): id_sub for nombre, id_sub, id_prog in subprogramas}

    @staticmethod
    def get_componentes_map():
        componentes = (
            Componentes.query
            .with_entities(Componentes.nombre, Componentes.id,  Componentes.idSubPrograma)
            .filter(Componentes.deleted == 0)
            .all()
        )
        return {(nombre, id_sub ) : id_com for nombre, id_com, id_sub in componentes}
    
    @staticmethod
    def get_beneficiarios_map():
        beneficiarios = (
            Beneficiarios.query
            .with_entities( Beneficiarios.CURP, Beneficiarios.RFC, Beneficiarios.id)
            .filter(Beneficiarios.deleted == 0)
            .all()
        )
        return {( curp, rfc ) : id_ben for curp, rfc, id_ben in beneficiarios}
    
    @staticmethod
    def get_acciones_map():
        tipos_acciones = (
            Acciones.query
            .with_entities(Acciones.nombre,Acciones.id)
            .filter(Acciones.deleted == 0)
            .all()
        )
        
        return { nombre: id_act for nombre, id_act in tipos_acciones}
    
    @staticmethod
    def get_tipos_beneficiarios_map():
        tipos_benefisarios = (
            
            TiposBeneficiarios.query
            .with_entities( TiposBeneficiarios.nombre, TiposBeneficiarios.id )
            .filter(TiposBeneficiarios.deleted == 0)
            .all()
        )
        return {nombre: id_tben for nombre, id_tben in tipos_benefisarios}
    
    @staticmethod
    def get_carpeta_beneficiarios_map():
        carpetas_beneficiarios = (
            CarpetaBeneficiarios.query
            .with_entities(CarpetaBeneficiarios.id, CarpetaBeneficiarios.mes, CarpetaBeneficiarios.anio, CarpetaBeneficiarios.idDependencia)
            .filter(CarpetaBeneficiarios.deleted == 0)
            .all()
        )
        
        return {(mes,anio,id_dep) : id_cbe for id_cbe, mes, anio, id_dep in carpetas_beneficiarios}