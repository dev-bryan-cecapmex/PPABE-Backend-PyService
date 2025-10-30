from ..database.connection import db

class Contacto(db.Model):
    __tablename__ = 'Contacto'
    
    id              = db.Column(db.String(36), primary_key = True)
    correo          = db.Column(db.String(255))
    telefono1       = db.Column(db.String(13))
    telefono2       = db.Column(db.String(13))
    idEstado        = db.Column(db.String(36))
    idMunicipio     = db.Column(db.String(36))
    Colonia         = db.Column(db.String(255))
    calle           = db.Column(db.String(255))
    numero          = db.Column(db.String(10))
    creador         = db.Column(db.String(36))
    modificador     = db.Column(db.String(36))
    fCreacion	    = db.Column(db.Date)
    fModificacion   = db.Column(db.Date)
    deleted         = db.Column(db.Boolean)
    idEstadoCivil   = db.Column(db.String(36))

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
