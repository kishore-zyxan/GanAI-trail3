from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.responses import JSONResponse
from typing import List, Dict
import os, json, re
import uuid

from extractor import extract_text
from llm_client import analyze_with_llm
from database import connect_mysql
from crud import insert_into_mysql, fetch_uploaded_files, delete_files_by_ids, update_or_insert_file
from utils import flatten_json, compute_diff
from models import FileDeleteRequest
from uuid import uuid4
from datetime import datetime


app = FastAPI(title="Universal Document Reader")



from fastapi import FastAPI, UploadFile, File, HTTPException, Body, BackgroundTasks
from fastapi.responses import JSONResponse



@app.post("/upload/")
async def upload_files(files: List[UploadFile] = File(...), background_tasks: BackgroundTasks = None):
    request_id = uuid4().hex
    upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_info_list = []

    for file in files:
        file_ext = os.path.splitext(file.filename)[1].lower()
        content = await file.read()
        file_id = uuid4().hex

        # Schedule background task
        background_tasks.add_task(
            process_file_background,
            file.filename,
            file_ext,
            content,
            request_id,
            file_id,
            upload_time
        )

        # Build minimal file info for response
        file_info_list.append({
            "file_name": file.filename,
            "file_id": file_id,
            "upload_time": datetime.strptime(upload_time, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M:%S")
        })

    return JSONResponse(content={
        "message": f"{len(file_info_list)} files uploaded. Processing will happen asynchronously.",
        "request_id": request_id,
        "file_info": file_info_list
    })


# 👇 Background task definition
def process_file_background(file_name, file_ext, content, request_id, file_id, upload_time):
    try:
        text = extract_text(file_ext, content)
        if not text:
            raise Exception("Text extraction failed.")

        llm_output = analyze_with_llm(text)
        match = re.search(r"\{(?:[^{}]*|\{(?:[^{}]*|\{.*})*})*}", llm_output, re.DOTALL)
        if not match:
            raise Exception("No JSON object found in LLM output.")

        json_str = match.group(0).strip()
        data = json.loads(json_str)
        flat_data = flatten_json(data)

        # Insert into MySQL
        insert_into_mysql(file_name, flat_data, request_id, file_id, upload_time)

    except Exception as e:
        # Log or handle the error
        print(f"[Error processing {file_name}]: {str(e)}")

@app.get("/files/")
def get_uploaded_files():
    return fetch_uploaded_files()

@app.get("/requests/")
def get_all_requests_summary():
    try:
        conn = connect_mysql()
        cursor = conn.cursor()

        query = """
            SELECT request_id, COUNT(*) as file_count, 
                   MIN(upload_date_time) as upload_date_time,
                   SUM(COALESCE(update_count, 0)) as total_update_count
            FROM documents
            GROUP BY request_id
        """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        return [
            {
                "request_id": row[0],
                "file_count": row[1],
                "upload_date_time": datetime.strptime(str(row[2]), "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M:%S") if row[2] else None,
                "total_update_count": row[3]
            } for row in results
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/requests/{request_id}")
def get_files_by_request_id(request_id: str):
    try:
        conn = connect_mysql()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, file_name, json_data, update_count, diff_data, request_id, file_id, upload_date_time
            FROM documents
            WHERE request_id = %s
        """, (request_id,))
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        if not results:
            raise HTTPException(status_code=404, detail="No files found for this request_id")

        return [
            {
                "id": row[0],
                "file_name": row[1],
                "json_data": json.loads(row[2]),
                "update_count": row[3],
                "diff_data": json.loads(row[4]) if row[4] else {},
                "request_id": row[5],
                "file_id": row[6],
                "upload_date_time": datetime.strptime(str(row[7]), "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M:%S")
            } for row in results
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/delete/")
def delete_files(req: FileDeleteRequest):
    delete_files_by_ids(req.file_ids)
    return {"message": "Selected files deleted successfully"}

@app.get("/file/{file_id}")
def get_file_by_id(file_id: int):
    try:
        conn = connect_mysql()
        cursor = conn.cursor()
        cursor.execute("SELECT file_name, json_data, update_count, diff_data FROM documents WHERE id = %s", (file_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="File not found")

        file_name, json_data, update_count, diff_data = result
        return {
            "file_id": file_id,
            "file_name": file_name,
            "json_data": json.loads(json_data),
            "update_count": update_count,
            "diff_data": json.loads(diff_data) if diff_data else None
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/file/{file_id}")
def update_file_json(file_id: int, new_data: Dict = Body(...)):
    try:
        conn = connect_mysql()
        cursor = conn.cursor()
        cursor.execute("SELECT json_data, update_count FROM documents WHERE id = %s", (file_id,))
        result = cursor.fetchone()

        if result:
            old_json_str, update_count = result
            old_json = json.loads(old_json_str)
            diff = compute_diff(old_json, new_data)

            update_count += 1
            cursor.execute("""
                UPDATE documents
                SET json_data = %s, update_count = %s, diff_data = %s
                WHERE id = %s
            """, (json.dumps(new_data), update_count, json.dumps(diff), file_id))
        else:
            # New file insert as PUT if not exists
            cursor.execute("""
                INSERT INTO documents (file_name, json_data, update_count, diff_data)
                VALUES (%s, %s, %s, %s)
            """, (f"file_{file_id}", json.dumps(new_data), 0, json.dumps({})))

        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "JSON data updated successfully", "updated_data": new_data, "diff": diff if result else {}}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))