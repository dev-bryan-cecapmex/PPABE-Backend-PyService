# from ..database.connection import db
# from ..models.beneficiarios import Beneficiarios
# 
# # Logger
# from ..utils.Logger import Logger
# 
# class BeneficiariosService:
#     @staticmethod
#     def add_beneficiario(id_beneficiario, id_user, data):
#         new_beneficiario = Beneficiarios(
#             id              = id_beneficiario,
#             CURP            = data.get("Curp"),
#             RFC	            = data.get("RFC"),
#             nombre	        = data.get("Nombre"),
#             aPaterno        = data.get("Apellido paterno"),
#             aMaterno        = data.get("Apellido Materno"),
#             fNacimiento	    = data.get("Fecha de Nacimiento"),
#             creador         = id_user,
#             modificador     = id_user,
#             deleted = 0
#         )
#         
#         db.session.add(new_beneficiario)
#         result = new_beneficiario.to_dict()
#         db.session.commit()
#         return result
#     
#     @staticmethod
#     def bulk_insert(rows):
#         try:
#             db.session.bulk_insert_mappings(Beneficiarios,rows)
#             db.session.commit()
#             return {"Nuevos Beneficiarios": len(rows)}
#         except Exception as ex:
#             db.session.rollback()
#             Logger.add_to_log("error", f"Error bulk_insert beneficiarios: {ex}")
from sqlalchemy import text

from ..database.connection import db
from ..models.beneficiarios import Beneficiarios
from ..utils.Logger import Logger


class BeneficiariosService:
    @staticmethod
    def add_beneficiario(id_beneficiario, id_user, data):
        """
        Inserción individual (se mantiene como estaba).
        """
        try:
            new_beneficiario = Beneficiarios(
                id=id_beneficiario,
                CURP=data.get("Curp"),
                RFC=data.get("RFC"),
                nombre=data.get("Nombre"),
                aPaterno=data.get("Apellido paterno"),
                aMaterno=data.get("Apellido Materno"),
                fNacimiento=data.get("Fecha de Nacimiento"),
                creador=id_user,
                modificador=id_user,
                deleted=0
            )

            db.session.add(new_beneficiario)
            db.session.commit()
            return new_beneficiario.to_dict()

        except Exception as ex:
            db.session.rollback()
            Logger.add_to_log("error", f"Error add_beneficiario: {ex}")
            raise

    @staticmethod
    def bulk_insert(rows, batch_size=1000, commit_every_batches=1):
        """
        Inserción masiva por lotes con COMMIT por batch (o cada N batches).
        Nota: aquí NO desactivamos FKs porque Beneficiarios suele ser tabla “base”.
        """
        if not rows:
            return {"Nuevos Beneficiarios": 0}

        total = 0
        batch_num = 0

        with db.engine.connect() as conn:
            trans = conn.begin()
            try:
                for i in range(0, len(rows), batch_size):
                    chunk = rows[i:i + batch_size]
                    batch_num += 1

                    conn.execute(Beneficiarios.__table__.insert(), chunk)
                    total += len(chunk)

                    Logger.add_to_log(
                        "info",
                        f"Beneficiarios: batch {batch_num} (+{len(chunk)}) total={total}"
                    )

                    if commit_every_batches and (batch_num % commit_every_batches == 0):
                        trans.commit()
                        trans = conn.begin()

                trans.commit()
                Logger.add_to_log("info", f"✅ 💾 {total} beneficiarios insertados exitosamente")
                return {"Nuevos Beneficiarios": total}

            except Exception as ex:
                try:
                    trans.rollback()
                except Exception:
                    pass
                Logger.add_to_log("error", f"Error bulk_insert beneficiarios: {ex}")
                raise
