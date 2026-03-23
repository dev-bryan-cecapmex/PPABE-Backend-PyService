# from ..database.connection import db
# from ..models.contacto  import Contacto
# 
# # Logger
# from ..utils.Logger import Logger
# 
# class ContactosService:
#     @staticmethod
#     def bulk_insert(rows):
#         try:
#             db.session.bulk_insert_mappings(Contacto, rows)
#             db.session.commit()
#             return {"Nuevos Beneficiarios": len(rows)}
#         except Exception as ex: 
#             db.session.rollback()
#             Logger.add_to_log("error", f"Error bulk_insert beneficiarios: {ex}")

from ..database.connection import db
from ..models.contacto import Contacto
from ..utils.Logger import Logger


class ContactosService:
    @staticmethod
    def bulk_insert(rows, batch_size=5000, commit_every_batches=1):
        """
        Inserción masiva por lotes con COMMIT por batch (o cada N batches).
        """
        if not rows:
            return {"Nuevos Contactos": 0}

        total = 0
        batch_num = 0

        with db.engine.connect() as conn:
            trans = conn.begin()
            try:
                for i in range(0, len(rows), batch_size):
                    chunk = rows[i:i + batch_size]
                    batch_num += 1

                    conn.execute(Contacto.__table__.insert(), chunk)
                    total += len(chunk)

                    Logger.add_to_log(
                        "info",
                        f"Contactos: batch {batch_num} (+{len(chunk)}) total={total}"
                    )

                    if commit_every_batches and (batch_num % commit_every_batches == 0):
                        trans.commit()
                        trans = conn.begin()

                trans.commit()
                Logger.add_to_log("info", f"✅ 💾 {total} contactos insertados exitosamente")
                return {"Nuevos Contactos": total}

            except Exception as ex:
                try:
                    trans.rollback()
                except Exception:
                    pass
                Logger.add_to_log("error", f"Error bulk_insert contactos: {ex}")
                raise
