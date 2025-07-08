from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import tempfile, os, shutil
import traceback

from convertor import process_stdf_file
from excel_transposer import run_transpose

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/convert/")
async def convert_stdf(file: UploadFile = File(...)):
    try:
        input_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".stdf")
        input_temp.write(await file.read())
        input_temp.close()
        input_path = input_temp.name
        output_path = input_path.replace(".stdf", ".xlsx")
        process_stdf_file(input_path, output_path)
        return FileResponse(output_path, filename="converted.xlsx")
    except Exception as e:
        print("[STDF ERROR]", str(e))
        raise HTTPException(status_code=500, detail="STDF conversion failed")

@app.post("/api/transpose/")
async def transpose_excel(file: UploadFile = File(...)):
    try:
        temp_path = f"temp_{file.filename}"
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        output_path = run_transpose(temp_path)

        return FileResponse(output_path, filename=os.path.basename(output_path))
    except Exception as e:
        print("[Excel ERROR]", str(e))
        traceback.print_exc()  # ✅ This will print the full error trace to the log
        return JSONResponse(status_code=500, content={"error": str(e)})

# Serve the HTML frontend
app.mount("/", StaticFiles(directory="static", html=True), name="static")
