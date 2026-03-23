
from sqlalchemy import text

from ..database.connection import db
from ..models.apoyo_integral import ApoyoIntegral
from ..utils.Logger import Logger


class ApoyoIntegralService:
    @staticmethod
    def bulk_insert(rows, batch_size=5000, commit_every_batches=1):
        """
        Inserta apoyos en lotes (batch) con COMMIT por batch (o cada N batches)
        y desactiva foreign_key_checks/unique_checks por SESIÓN durante la carga.

        rows: List[Dict[str, Any]]  (mappings para Apoyos)
        batch_size: tamaño de lote (ej. 2000-5000)
        commit_every_batches: cada cuántos batches hacer commit (1 = cada batch)
        """
        if not rows:
            return {"Nuevos Apoyos": 0}

        total = 0
        batch_num = 0

        # Usamos una conexión dedicada para que el SET SESSION sea 100% por sesión.
        with db.engine.connect() as conn:
            trans = conn.begin()
            try:
                # ✅ Desactivar checks SOLO en esta conexión/sesión
                conn.execute(text("SET SESSION foreign_key_checks = 0"))
                conn.execute(text("SET SESSION unique_checks = 0"))

                for i in range(0, len(rows), batch_size):
                    chunk = rows[i:i + batch_size]
                    batch_num += 1

                    # executemany real (rápido)
                    conn.execute(ApoyoIntegral.__table__.insert(), chunk)

                    total += len(chunk)
                    Logger.add_to_log(
                        "info",
                        f"Apoyos: batch {batch_num} (+{len(chunk)}) total={total}"
                    )

                    # ✅ COMMIT por batch (o cada N batches)
                    if commit_every_batches and (batch_num % commit_every_batches == 0):
                        trans.commit()
                        trans = conn.begin()

                # Por si quedó una transacción abierta sin commit
                trans.commit()
                Logger.add_to_log("info", f"✅ 💾 {total} apoyos insertados exitosamente")
                return {"Nuevos Apoyos": total}

            except Exception as ex:
                try:
                    trans.rollback()
                except Exception:
                    pass
                Logger.add_to_log("error", f"Error bulk_insert apoyos: {ex}")
                raise

            finally:
                # ✅ IMPORTANTÍSIMO: restaurar checks antes de devolver la conexión al pool
                try:
                    conn.execute(text("SET SESSION foreign_key_checks = 1"))
                    conn.execute(text("SET SESSION unique_checks = 1"))
                except Exception:
                    # si fallara, no bloqueamos el cierre, pero lo ideal es que no falle
                    pass
