from ..database.connection import db

class EstadosCiviles(db.Model):
    __tablename__ = 'estadosciviles'
    id      = db.Column(db.String(36), primary_key = True)
    nombre  = db.Column(db.String(100))
    deleted = db.Column(db.String(4))
    # `id` CHAR(36) NOT NULL DEFAULT uuid() COLLATE 'utf8mb4_spanish_ci',
	# `nombre` VARCHAR(100) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `creador` CHAR(36) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `modificador` CHAR(36) NOT NULL COLLATE 'utf8mb4_spanish_ci',
	# `fCreacion` DATE NOT NULL DEFAULT current_timestamp(),
	# `fModificacion` DATE NOT NULL DEFAULT current_ti
    
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}