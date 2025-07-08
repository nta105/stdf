from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import tempfile
import os

from convertor import process_stdf_file
from excel_transposer import run_transpose

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/ping")
def ping():
    return {"status": "ok"}

@app.post("/api/convert-stdf/")
async def convert_stdf(file: UploadFile = File(...)):
    input_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".stdf")
    input_path = input_temp.name
    input_temp.write(await file.read())
    input_temp.close()

    output_path = input_path.replace(".stdf", ".xlsx")
    process_stdf_file(input_path, output_path)

    return FileResponse(
        output_path,
        filename="converted_stdf.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

@app.post("/api/transpose-excel/")
async def transpose_excel(file: UploadFile = File(...)):
    input_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    input_path = input_temp.name
    input_temp.write(await file.read())
    input_temp.close()

    output_path = run_transpose(input_path)

    return FileResponse(
        output_path,
        filename=os.path.basename(output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

app.mount("/", StaticFiles(directory="static", html=True), name="static")
