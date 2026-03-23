from ..models.historial_carga import HistorialCarga
from ..database.connection import db

class HistoriaCargaService:
    
    @staticmethod
    def insertCarga(id, idDependencia, idUsuarioCargaMasiva):
        try:
            registroCarga = HistorialCarga(
                id=id,
                idUsuarioCargaMasiva=idUsuarioCargaMasiva,
                idDependencia=idDependencia
            )
            db.session.add(registroCarga)
            db.session.commit()
            return registroCarga
        except Exception as e:
            db.session.rollback()
            raise e