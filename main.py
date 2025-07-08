from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import tempfile
import os

from convertor import process_stdf_file  # ✅ your real logic now

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,

)


@app.get("/")
async def root():
    with open("static/index.html", "r") as f:
        html = f.read()
    return HTMLResponse(content=html)

@app.post("/api/convert/")
async def convert(file: UploadFile = File(...)):
    print("[INFO] Upload received:", file.filename)
    input_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".stdf")
    input_path = input_temp.name
    input_temp.write(await file.read())
    input_temp.close()
    print("[INFO] File saved to:", input_path)

    try:
        output_path = input_path.replace(".stdf", ".xlsx")
        print("[INFO] Converting...")
        process_stdf_file(input_path, output_path)
        print("[INFO] Done converting to:", output_path)
    except Exception as e:
        print("[ERROR]", e)
        raise

    return FileResponse(
        output_path,
        filename="converted.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


app.mount("/static", StaticFiles(directory="static", html=True), name="static")
