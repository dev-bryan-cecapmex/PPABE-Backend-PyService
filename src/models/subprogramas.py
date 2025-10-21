from ..database.connection import db

class Subprogramas(db.Model):
    __tablename__ = 'SubPrograma'
    
    id              = db.Column(db.Integer, primary_key=True)
    nombre          = db.Column(db.String(255))
    idPrograma      = db.Column(db.String(36))
    creador         = db.Column(db.String(36))
    modificador	    = db.Column(db.String(36))
    fCreacion	    = db.Column(db.Date)
    fModificacion   = db.Column(db.Date)
    deleted         = db.Column(db.Boolean)
    
    def to_dict(self):
        return { c.name: getattr(self, c.name) for c in self.__table__.columns}