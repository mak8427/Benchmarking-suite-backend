from fastapi import FastAPI, Request

app = FastAPI()


def launch_job(bucket: str, key: str):
    name = f"duckdb-{int(time.time())}"
    subprocess.run(["kubectl","create","job",name,"--image=localhost/duckdb-analysis:latest"], check=True)

@app.post("/minio")
async def minio(req: Request, bg: BackgroundTasks):
    e = await req.json()
    r = e["Records"][0]
    bg.add_task(launch_job, r["s3"]["bucket"]["name"], r["s3"]["object"]["key"])
    return {"ok": True}
