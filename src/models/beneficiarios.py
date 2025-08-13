from ..database.connection import db

class Beneficiarios(db.Model):
    __tablename__ = 'beneficiarios'
    
    id              = db.Column(db.String(36), primary_key = True)
    CURP            = db.Column(db.String(18), unique=True, nullable=False)
    RFC	            = db.Column(db.String(13), unique=True, nullable=False)
    regimenCapital  = db.Column(db.String(255))
    actividad       = db.Column(db.String(255))
    nombreComercial = db.Column(db.String(255))
    razonSocial     = db.Column(db.String(255))
    idSexo          = db.Column(db.String(36))
    nombre	        = db.Column(db.String(255))
    aPaterno        = db.Column(db.String(255))
    aMaterno        = db.Column(db.String(255))
    fNacimiento	    = db.Column(db.Date)
    creador	        = db.Column(db.String(36))
    modificador	    = db.Column(db.String(36))
    fCreacion	    = db.Column(db.Date)
    fModificacion   = db.Column(db.Date)
    deleted         = db.Column(db.Boolean)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
