from ..database.connection import db

class Municipios(db.Model):
    __tablename__ = 'Municipios'
    
    id          = db.Column(db.String(36), primary_key = True)
    nombre      = db.Column(db.String(255))
    idEstado    = db.Column(db.String(36))
    deleted     = db.Column(db.String(4))
    # `id` CHAR(36) NOT NULL DEFAULT uuid() COLLATE 'utf8mb4_spanish_ci',
	# `nombre` VARCHAR(255) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `idEstado` CHAR(36) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `creador` CHAR(36) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `modificador` CHAR(36) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `fCreacion` DATETIME NOT NULL DEFAULT current_timestamp(),
	# `fModificacion` DATETIME NOT NULL DEFAULT current_timestamp(),
	# `deleted` TINYINT(4) NOT NULL DEFAULT '0',
    
    def to_dict(self):
        return {c.name:getattr(self, c.name) for c in self.__table__.columns}
