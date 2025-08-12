from ..models.beneficiarios import Beneficiarios
from ..database.connection import db

class BeneficiariosService:
    @staticmethod
    def add_beneficiario(data):
        new_beneficiario = Beneficiarios(
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