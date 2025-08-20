# Importar los modelos para buscar su informacion
from ..models.dependencias  import Dependencias
from ..models.programas     import Programas
from ..models.componentes   import Componentes

from ..models.sexos             import Sexos
from ..models.estados           import Estados
from ..models.municipios        import Municipios
from ..models.colonias          import Colonias
from ..models.estados_civiles   import EstadosCiviles



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
    def get_componentes_map():
        componentes = (
            Componentes.query
            .with_entities(Componentes.nombre, Componentes.id,  Componentes.idPrograma)
            .filter(Componentes.deleted == 0)
            .all()
        )
        return {(nombre, id_prog ) : id_com for nombre, id_com, id_prog in componentes}
    
    @staticmethod
    def get_beneficiarios_map():
        beneficiarios = (
            Beneficiarios.query
            .with_entities( Beneficiarios.CURP, Beneficiarios.RFC, Beneficiarios.id)
            .filter(Beneficiarios.deleted == 0)
            .all()
        )
        return {( curp, rfc ) : id_ben for curp, rfc, id_ben in beneficiarios}