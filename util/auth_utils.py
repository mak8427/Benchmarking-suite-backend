import re
from typing import Annotated

import jwt
from fastapi import HTTPException, Header
SECRET = b"..."

def current_user(
    authorization: Annotated[str | None, Header(alias="Authorization")] = None) -> dict:
    if not authorization:
        raise HTTPException(401, "missing Authorization header")
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise ValueError("bad scheme")
        payload = jwt.decode(token, SECRET, algorithms=["HS256"])
        return {"username": payload["sub"]}
    except Exception:
        raise HTTPException(401, "invalid or expired token")

def sanitize(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    if not name or ".." in name or name.startswith("/"):
        raise HTTPException(400, "invalid filename")
    return name
