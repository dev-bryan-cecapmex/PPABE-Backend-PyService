from ..database.connection import db
from ..models.contacto  import Contacto

# Logger
from ..utils.Logger import Logger

class ContactosService:
    @staticmethod
    def bulk_insert(rows):
        try:
            db.session.bulk_insert_mappings(Contacto, rows)
            db.session.commit()
            return {"Nuevos Beneficiarios": len(rows)}
        except Exception as ex: 
            db.session.rollback()
            Logger.add_to_log("error", f"Error bulk_insert beneficiarios: {ex}")