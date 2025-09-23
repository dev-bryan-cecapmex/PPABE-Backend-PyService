from ..database.connection import db

class Anio(db.Model):
    __tablename__ = 'Anios'
    
    id = db.Column(db.Integer, primary_key = True)
    creador  = db.Column(db.String(36), nullable=False)
    modificador = db.Column(db.String(36), nullable=False)
    fCreacion = db.Column(db.Date)
    fModificacion  = db.Column(db.Date)
    deleted  = db.Column(db.String(50))
    
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}