import time, jwt, secrets
from fastapi import FastAPI, HTTPException, status, Header
from passlib.hash import argon2
from pydantic import BaseModel, Field
from storage.minio_client import make_bucket_token, presigned_put_url
import logging


app = FastAPI()
SECRET = b"..."
TOKENS = {}
USERS = {"alice": argon2.hash("pass")}

def logging_config():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("process.log", encoding="utf-8"),  # Note the encoding parameter
            logging.StreamHandler(),
        ],
    )
if __name__ == "__main__":
    logging_config()
    logging.info("Logging initialized.")
    logging.info("Application started with initial users: %s", list(USERS.keys()))

class UserCreate(BaseModel):
    username: str = Field(..., min_length=5, max_length=20)
    password: str = Field(..., min_length=8, max_length=128)

@app.get("/")
async def root():
    logging.info("Root endpoint accessed.")
    return {"message": "Hello World"}

def make_access(sub):
    """
    Create a short-lived access token for the given subject.
    :param sub:
    :return:
    """
    logging.info("Creating access token for: %s", sub)
    return jwt.encode({"sub": sub, "scope": "upload", "exp": time.time() + 600}, SECRET, "HS256")

@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def register(payload: UserCreate):
    logging.info("Register endpoint called for username: %s", payload.username)
    if payload.username in USERS:
        logging.warning("Attempt to register already existing username: %s", payload.username)
        raise HTTPException(status_code=409, detail="Username already registered")

    USERS[payload.username] = argon2.hash(payload.password)
    rid = secrets.token_urlsafe(32)
    TOKENS[rid] = {"sub": payload.username, "exp": time.time() + 30 * 86400}
    logging.info("User %s created with refresh token id: %s", payload.username, rid)

    # Save user to plain text, for now
    with open("users.txt", "a") as f:
        f.write(f"{payload.username}:{USERS[payload.username]}\n")
    logging.info("User %s saved to users.txt", payload.username)

    # Create per-user bucket and mint a bucket token for uploads
    bucket_token, bucket_name = make_bucket_token(payload.username)
    logging.info("Bucket created for user %s: %s", payload.username, bucket_name)

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
    logging.info("Login attempt for username: %s", u)
    try:
        with open("users.txt", "r") as f:
            for line in f:
                user, hashv = line.split(":")
                USERS[user] = hashv.strip()
    except FileNotFoundError:
        logging.error("users.txt not found during login attempt.")
        pass

    if u not in USERS or not argon2.verify(p, USERS[u]):
        logging.warning("Login failed for user: %s", u)
        raise HTTPException(401)

    rid = secrets.token_urlsafe(32)
    TOKENS[rid] = {"sub": u, "exp": time.time() + 30 * 86400}
    logging.info("Login successful for user: %s, refresh id: %s", u, rid)
    return {"access": make_access(u), "refresh": rid}

@app.post("/auth/refresh")
def refresh(rid: str):
    logging.info("Refresh token requested: %s", rid)
    t = TOKENS.get(rid)
    if not t or t["exp"] < time.time():
        logging.warning("Refresh token invalid or expired: %s", rid)
        raise HTTPException(401)
    new = secrets.token_urlsafe(32)
    TOKENS[new] = t
    TOKENS.pop(rid, None)
    logging.info("Refresh token rotated: old=%s, new=%s", rid, new)
    return {"access": make_access(t["sub"]), "refresh": new}

# Exchange a bucket token for a presigned PUT URL
@app.post("/storage/upload_url")
def create_upload_url(object_name: str, x_bucket_token: str = Header(..., alias="X-Bucket-Token")):
    logging.info("Presigned PUT URL requested for object: %s", object_name)
    try:
        url = presigned_put_url(x_bucket_token, object_name, expires_seconds=600)
        logging.info("Presigned PUT URL generated for object: %s", object_name)
        return {"url": url, "expires_in": 600}
    except Exception as e:
        logging.error("Failed to generate presigned PUT URL: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))