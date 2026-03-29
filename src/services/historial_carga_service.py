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

    @staticmethod
    def updateEstatusCarga(id, estatus):
        try:
            db.session.expire_all()  # ← limpia sesión sucia del bulk_insert
            
            registroCarga = HistorialCarga.query.filter_by(id=id).first()
            
            if registroCarga is None:
                raise ValueError(f"No se encontró HistorialCarga con id: {id}")
            
            if estatus is not None:
                registroCarga.estatus = estatus
                db.session.merge(registroCarga)
                db.session.flush()
                
            db.session.commit()
            return registroCarga

        except Exception as e:
            db.session.rollback()
            raise e