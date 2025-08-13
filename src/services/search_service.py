# Importar los modelos para buscar su informacion
from ..models.dependencias  import Dependencias
from ..models.programas     import Programas
from ..models.componentes   import Componentes

from ..models.beneficiarios import Beneficiarios

from ..database.connection  import db

class SearchService:
    
    @staticmethod
    def search_dependencia(name_dependencia): 
        result = (
            Dependencias.query
            .with_entities(Dependencias.id)
            .filter(Dependencias.nombre == name_dependencia)
            .first()
        )
        
        return result.id if result else None
    
    @staticmethod
    def search_programa(name_programa, id_dependencia):
        result = (
            Programas.query
            .with_entities(Programas.id)
            .filter((Programas.nombre == name_programa) & (Programas.idDependencia == id_dependencia))
            .first()
        )
        
        return result.id if result else None
    
    @staticmethod
    def search_componente(name_componente, id_Programa):
        result = (
            Componentes.query
            .with_entities(Componentes.id)
            .filter((Componentes.nombre == name_componente) & (Componentes.idPrograma == id_Programa))
            .first()
        )
        
        return result.id if result else None
    
    @staticmethod
    def search_exitencia(curp,rfc):
        result = (
            Beneficiarios.query
            .with_entities(Beneficiarios.id)
            .filter((Beneficiarios.CURP == curp) | (Beneficiarios.RFC == rfc))
            .first()
        )
        
        return result.id if result else False