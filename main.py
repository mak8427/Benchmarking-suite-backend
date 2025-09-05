import time, jwt, secrets
from fastapi import FastAPI, HTTPException, status, Header
from passlib.hash import argon2
from pydantic import BaseModel, Field
from storage.minio_client import make_bucket_token, presigned_put_url
app = FastAPI()
SECRET = b"..."
TOKENS = {}
USERS = {"alice": argon2.hash("pass")}


class UserCreate(BaseModel):
    username: str = Field(..., min_length=5, max_length=20)
    password: str = Field(..., min_length=8, max_length=128)

@app.get("/")
async def root():
    return {"message": "Hello World"}

def make_access(sub):
    """
    Create a short-lived access token for the given subject.
    :param sub:
    :return:
    """
    return jwt.encode({"sub": sub, "scope": "upload", "exp": time.time() + 600}, SECRET, "HS256")

@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def register(payload: UserCreate):
    if payload.username in USERS:
        raise HTTPException(status_code=409, detail="Username already registered")

    USERS[payload.username] = argon2.hash(payload.password)
    rid = secrets.token_urlsafe(32)
    TOKENS[rid] = {"sub": payload.username, "exp": time.time() + 30 * 86400}

    # Save user to plain text, for now
    with open("users.txt", "a") as f:
        f.write(f"{payload.username}:{USERS[payload.username]}\n")

    # Create per-user bucket and mint a bucket token for uploads
    bucket_token, bucket_name = make_bucket_token(payload.username)

    return {
        "access": make_access(payload.username),
        "refresh": rid,
        "bucket": bucket_name,
        "bucket_token": bucket_token,
    }

@app.post("/auth/password")
def login(u: str, p: str):
    """
    Authenticate user and return access and refresh tokens.
    30 day refresh token, 10 minute access token.
    :param u:
    :param p:
    :return:
    """

    try:
        with open("users.txt", "r") as f:
            for line in f:
                user, hashv = line.split(":")
                USERS[user] = hashv.strip()
    except FileNotFoundError:
        pass

    if u not in USERS or not argon2.verify(p, USERS[u]):
        raise HTTPException(401)

    rid = secrets.token_urlsafe(32)
    TOKENS[rid] = {"sub": u, "exp": time.time() + 30 * 86400}
    return {"access": make_access(u), "refresh": rid}

@app.post("/auth/refresh")
def refresh(rid: str):
    t = TOKENS.get(rid)
    if not t or t["exp"] < time.time():
        raise HTTPException(401)
    new = secrets.token_urlsafe(32)
    TOKENS[new] = t
    TOKENS.pop(rid, None)
    return {"access": make_access(t["sub"]), "refresh": new}

# Exchange a bucket token for a presigned PUT URL
@app.post("/storage/upload_url")
def create_upload_url(object_name: str, x_bucket_token: str = Header(..., alias="X-Bucket-Token")):
    try:
        url = presigned_put_url(x_bucket_token, object_name, expires_seconds=600)
        return {"url": url, "expires_in": 600}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))