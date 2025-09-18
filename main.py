import os
import re
import time, jwt, secrets
from datetime import timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, status, Header, Query, Depends
from passlib.hash import argon2
from pydantic import BaseModel, Field
from storage.minio_client import BUCKET, PUBLIC_MINIO, ADMIN_MINIO
import logging

from util.auth_utils import sanitize, current_user

# Configure logging at module level
LOG_FILE = Path(os.getenv("LOG_FILE_PATH", "process.log"))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

app = FastAPI()
SECRET = b"..."
TOKENS = {}
USERS = {"alice": argon2.hash("pass")}
USERS_FILE = Path(os.getenv("USERS_FILE_PATH", "users.txt"))


def get_public_minio_client():
    return PUBLIC_MINIO


def get_admin_minio_client():
    return ADMIN_MINIO


class UserCreate(BaseModel):
    username: str = Field(..., min_length=5, max_length=20)
    password: str = Field(..., min_length=8, max_length=128)


@app.get("/")
async def root():
    logger.info("Root endpoint accessed")
    return {"message": "Hello World"}


def make_access(sub):
    """
    Create a short-lived access token for the given subject.
    :param sub:
    :return:
    """
    logger.debug(f"Creating access token for user: {sub}")
    return jwt.encode({"sub": sub, "scope": "upload", "exp": time.time() + 600}, SECRET, "HS256")


@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def register(payload: UserCreate):
    logger.info(f"Registration attempt for username: {payload.username}")

    if payload.username in USERS:
        logger.warning(f"Registration failed: Username {payload.username} already exists")
        raise HTTPException(status_code=409, detail="Username already registered")

    USERS[payload.username] = argon2.hash(payload.password)
    rid = secrets.token_urlsafe(32)
    TOKENS[rid] = {"sub": payload.username, "exp": time.time() + 30 * 86400}

    # Save user to plain text, for now
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with USERS_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{payload.username}:{USERS[payload.username]}\n")
        logger.debug(f"User {payload.username} saved to users.txt")


    logger.info(f"User {payload.username} registered successfully")
    return {
        "access": make_access(payload.username),
        "refresh": rid
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
    logger.info(f"Login attempt for username: {u}")

    try:
        with USERS_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                user, hashv = line.split(":")
                USERS[user] = hashv.strip()
        logger.debug("User data loaded from users.txt")
    except FileNotFoundError:
        logger.warning("users.txt not found, using in-memory user data only")
        pass

    if u not in USERS or not argon2.verify(p, USERS[u]):
        logger.warning(f"Failed login attempt for username: {u}")
        raise HTTPException(401)

    rid = secrets.token_urlsafe(32)
    TOKENS[rid] = {"sub": u, "exp": time.time() + 30 * 86400}
    logger.info(f"User {u} logged in successfully")
    return {"access": make_access(u), "refresh": rid}


@app.post("/auth/refresh")
def refresh(rid: str):
    logger.info("Token refresh attempt")
    t = TOKENS.get(rid)
    if not t or t["exp"] < time.time():
        logger.warning(f"Invalid or expired refresh token")
        raise HTTPException(401)

    new = secrets.token_urlsafe(32)
    TOKENS[new] = t
    TOKENS.pop(rid, None)
    logger.info(f"Successfully refreshed token for user: {t['sub']}")
    return {"access": make_access(t["sub"]), "refresh": new}




@app.post("/storage/presign/upload")
def create_upload_url(
    object_name: str = Query(...),
    user=Depends(current_user),
    minio_client=Depends(get_public_minio_client),
):
    safe = sanitize(object_name)
    key = f"{user['username']}/{safe}"  # prefix by caller identity
    try:
        url = minio_client.presigned_put_object(BUCKET, key, expires=timedelta(minutes=30))
        logger.info(f"presigned PUT for {key}")
        return {"key": key, "url": url, "expires_in": 600}
    except Exception as e:
        logger.error(f"presign failed for {key}: {e}")
        raise HTTPException(400, "failed to create upload URL")



@app.get("/storage/presign/download")
def create_download_url(
    object_name: str = Query(...),
    user=Depends(current_user),
    minio_client=Depends(get_public_minio_client),
):
    safe = sanitize(object_name)
    key = f"{user['username']}/{safe}"
    try:
        url = minio_client.presigned_get_object(BUCKET, key, expires=timedelta(minutes=30))
        logger.info(f"presigned GET for {key}")
        return {"key": key, "url": url, "expires_in": 600}
    except Exception as e:
        logger.error(f"download presign failed for {key}: {e}")
        raise HTTPException(400, "failed to create download URL")


@app.get("/storage/list")
def list_objects(
    user=Depends(current_user),
    minio_client=Depends(get_admin_minio_client),
):
    prefix = f"{user['username']}/"
    objects = []
    try:
        for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True):
            if not getattr(obj, "object_name", "").startswith(prefix):
                continue
            objects.append(
                {
                    "key": obj.object_name,
                    "size": getattr(obj, "size", 0),
                    "last_modified": getattr(obj, "last_modified", None).isoformat()
                    if getattr(obj, "last_modified", None)
                    else None,
                }
            )
        logger.info(f"listed objects for {user['username']}: {len(objects)} items")
        return {"objects": objects}
    except Exception as e:
        logger.error(f"list objects failed for {user['username']}: {e}")
        raise HTTPException(400, "failed to list objects")



# This block only runs when the script is executed directly
if __name__ == "__main__":
    logger.info("Starting application in direct execution mode")
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
