# Importar los modelos para buscar su informacion
from ..models.dependencias  import Dependencias
from ..models.programas     import Programas

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
    def search_programas(name_programa, id_dependencia):
        result = (
            Programas.query
            .with_entities(Programas.id)
            .filter((Programas.nombre == name_programa) & (Programas.idDependencia == id_dependencia))
            .first()
        )
        
        return result.id if result else None