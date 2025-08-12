from ..database.connection import db

class Dependencias(db.Model):
    __tablename__ = 'dependencias'
    
    id              = db.Column(db.Integer, primary_key = True)
    nombre          = db.Column(db.String(255))
    creador	        = db.Column(db.String(50))
    modificador	    = db.Column(db.String(50))
    fCreacion	    = db.Column(db.Date)
    fModificacion   = db.Column(db.Date)
    deleted         = db.Column(db.Boolean)
    
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}