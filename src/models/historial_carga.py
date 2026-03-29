from ..database.connection import db


class HistorialCarga(db.Model):
    __tablename__ = 'HistorialCarga'

    id                          = db.Column(db.String(36), primary_key=True)
    idDependencia               = db.Column(db.String(36))
    idUsuarioCargaMasiva        = db.Column(db.String(36))
    estatus                     = db.Column(db.Enum('EXITOSA', 'FALLIDA'), nullable=True, default='FALLIDA')
   

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
