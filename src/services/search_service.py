# Importar los modelos para buscar su informacion
from ..models.dependencias  import Dependencias
from ..models.programas     import Programas
from ..models.componentes   import Componentes

from ..models.sexos         import Sexos

from ..models.beneficiarios import Beneficiarios


from ..database.connection  import db

class SearchService:
    
    # @staticmethod
    # def search_dependencia(name_dependencia): 
    #     result = (
    #         Dependencias.query
    #         .with_entities(Dependencias.id)
    #         .filter(Dependencias.nombre == name_dependencia)
    #         .first()
    #     )
        
    #     return result.id if result else None

    # @staticmethod
    # def search_programa(name_programa, id_dependencia):
    #     result = (
    #         Programas.query
    #         .with_entities(Programas.id)
    #         .filter((Programas.nombre == name_programa) & (Programas.idDependencia == id_dependencia))
    #         .first()
    #     )
        
    #     return result.id if result else None
    
    # @staticmethod
    # def search_componente(name_componente, id_Programa):
    #     result = (
    #         Componentes.query
    #         .with_entities(Componentes.id)
    #         .filter((Componentes.nombre == name_componente) & (Componentes.idPrograma == id_Programa))
    #         .first()
    #     )
        
    #     return result.id if result else None
    
    # @staticmethod
    # def search_exitencia(curp,rfc):
    #     result = (
    #         Beneficiarios.query
    #         .with_entities(Beneficiarios.id)
    #         .filter(
    #             (Beneficiarios.CURP == curp) | (Beneficiarios.RFC == rfc))
    #         .first()
    #     )
        
    #     return result.id if result else False
    
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