from ..models.beneficiarios import Beneficiarios
from ..database.connection import db

# Logger
from ..utils.Logger import Logger

class BeneficiariosService:
    @staticmethod
    def add_beneficiario(id_beneficiario, data):
        new_beneficiario = Beneficiarios(
            id              = id_beneficiario,
            CURP            = data.get("Curp"),
            RFC	            = data.get("RFC"),
            nombre	        = data.get("Nombre"),
            aPaterno        = data.get("Apellido paterno"),
            aMaterno        = data.get("Apellido Materno"),
            fNacimiento	    = data.get("Fecha de Nacimiento"),
            deleted = 0
        )
        
        db.session.add(new_beneficiario)
        result = new_beneficiario.to_dict()
        db.session.commit()
        return result
    
    @staticmethod
    def bulk_insert(rows):
        try:
            db.session.bulk_insert_mappings(Beneficiarios,rows)
            db.session.commit()
            return {"Nuevos Beneficiarios": len(rows)}
        except Exception as ex:
            db.session.rollback()
            Logger.add_to_log("error", f"Error bulk_insert beneficiarios: {ex}")