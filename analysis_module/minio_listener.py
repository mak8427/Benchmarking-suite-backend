from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/minio")
async def minio_event(req: Request):
    event = await req.json()
    print("MinIO event:", event)
    return {"ok": True}
