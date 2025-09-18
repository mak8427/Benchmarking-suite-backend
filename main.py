import re
import time, jwt, secrets
from fastapi import FastAPI, HTTPException, status, Header, Query, Depends
from passlib.hash import argon2
from pydantic import BaseModel, Field
from storage.minio_client import MINIO, BUCKET
import logging

from util.auth_utils import sanitize, current_user

# Configure logging at module level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("process.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

app = FastAPI()
SECRET = b"..."
TOKENS = {}
USERS = {"alice": argon2.hash("pass")}


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
    with open("users.txt", "a") as f:
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
        with open("users.txt", "r") as f:
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
def create_upload_url(object_name: str = Query(...), user=Depends(current_user)):
    safe = sanitize(object_name)
    key = f"{user['username']}/{safe}"  # prefix by caller identity
    try:
        url = MINIO.presigned_put_object(BUCKET, key, expires=600)
        logger.info(f"presigned PUT for {key}")
        return {"key": key, "url": url, "expires_in": 600}
    except Exception as e:
        logger.error(f"presign failed for {key}: {e}")
        raise HTTPException(400, "failed to create upload URL")



# This block only runs when the script is executed directly
if __name__ == "__main__":
    logger.info("Starting application in direct execution mode")
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)