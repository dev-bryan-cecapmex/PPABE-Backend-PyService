from ..database.connection import db

class DependenciaProgramaAnio(db.Model):
    __tablename__ = 'DependenciaProgramaAnio'
    
    id              = db.Column(db.String(36), primary_key=True)
    idDependencia   = db.Column(db.String(36), nullable=False)
    idPrograma      = db.Column(db.String(36), nullable=False)
    anio            = db.Column(db.Integer, nullable=False)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
