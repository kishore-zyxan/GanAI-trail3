from database import connect_mysql
import pymysql
import json
from datetime import datetime

def insert_into_mysql(file_name, json_data, request_id, file_id, upload_date_time):
    conn = connect_mysql()
    cursor = conn.cursor()
    query = """
        INSERT INTO documents (file_name, json_data, request_id, file_id, upload_date_time)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        file_name,
        json.dumps(json_data),
        request_id,
        file_id,
        upload_date_time
    ))
    conn.commit()
    cursor.close()
    conn.close()



def fetch_uploaded_files():
    conn = connect_mysql()
    cursor = conn.cursor()
    cursor.execute("SELECT id, file_name FROM documents")
    files = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"id": row[0], "file_name": row[1]} for row in files]



def delete_files_by_ids(file_ids):
    conn = connect_mysql()
    cursor = conn.cursor()
    format_strings = ','.join(['%s'] * len(file_ids))
    query = f"DELETE FROM documents WHERE id IN ({format_strings})"
    cursor.execute(query, tuple(file_ids))
    conn.commit()
    cursor.close()
    conn.close()


def get_existing_file(file_name):
    conn = connect_mysql()
    cursor = conn.cursor()
    cursor.execute("SELECT json_data, update_count FROM documents WHERE file_name = %s", (file_name,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result

def update_existing_file(file_name, new_json, diff_json):
    conn = connect_mysql()
    cursor = conn.cursor()
    query = """
        UPDATE documents
        SET json_data = %s,
            update_count = update_count + 1,
            difference_json = %s
        WHERE file_name = %s
    """
    cursor.execute(query, (json.dumps(new_json), json.dumps(diff_json), file_name))
    conn.commit()
    cursor.close()
    conn.close()




def update_or_insert_file(file_name, new_data):
    conn = connect_mysql()
    cursor = conn.cursor()

    cursor.execute("SELECT json_data, update_count FROM documents WHERE file_name = %s", (file_name,))
    row = cursor.fetchone()

    if row:
        # Existing record
        old_data = json.loads(row[0])
        update_count = row[1] + 1

        # Find difference
        diff = {k: v for k, v in new_data.items() if old_data.get(k) != v}

        update_query = """
            UPDATE documents
            SET json_data = %s,
                update_count = %s,
                diff_data = %s
            WHERE file_name = %s
        """
        cursor.execute(update_query, (
            json.dumps(new_data),
            update_count,
            json.dumps(diff),
            file_name
        ))

        conn.commit()
        result = {
            "message": f"{file_name} updated successfully.",
            "update_count": update_count,
            "difference": diff
        }
    else:
        # Insert new
        insert_query = """
            INSERT INTO documents (file_name, json_data, update_count, diff_data)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            file_name,
            json.dumps(new_data),
            0,
            json.dumps({})
        ))
        conn.commit()
        result = {
            "message": f"{file_name} inserted as new.",
            "update_count": 0,
            "difference": {}
        }

    cursor.close()
    conn.close()
    return result