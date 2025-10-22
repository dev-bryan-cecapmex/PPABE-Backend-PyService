from ..database.connection import db
from ..models.apoyos import Apoyos

# Logger
from ..utils.Logger import Logger

class ApoyosService:
    @staticmethod
    def bulk_insert(rows):
        try:
            db.session.bulk_insert_mappings(Apoyos,rows)
            db.session.commit()
            return {"Nuevos Apoyos": len(rows)}
        except Exception as ex:
            db.session.rollback()
            Logger.add_to_log("error", f"Error bulk_insert beneficiarios: {ex}")