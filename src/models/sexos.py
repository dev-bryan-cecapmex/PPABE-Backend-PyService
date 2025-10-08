from ..database.connection import db

class Sexos(db.Model):
    __tablename__ = 'Sexos'
    
    id              = db.Column(db.Integer, primary_key = True)
    nombre          = db.Column(db.String(100))
    creador         = db.Column(db.String(36), nullable=False)
    modificador     = db.Column(db.String(36), nullable=False)
    fCreacion       = db.Column(db.Date)
    fModificacion   = db.Column(db.Date)
    deleted         = db.Column(db.String(4))
    
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}