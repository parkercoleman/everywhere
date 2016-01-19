from main.model import get_connection


def vacuum_full():
    conn = get_connection()
    try:
        conn.autocommit = True
        conn.cursor().execute("VACUUM FULL gis.places")
        conn.cursor().execute("VACUUM FULL gis.roads")
    finally:
        conn.close()


def execute_import_statements(sql_lines):
        conn = get_connection()
        c = conn.cursor()
        try:
            i = 0
            for sql_line in sql_lines:
                if not sql_line.startswith("INSERT"):
                    continue

                c.execute(sql_line)
                if i % 50 == 0:
                    conn.commit()

            conn.commit()
        finally:
            conn.close()
