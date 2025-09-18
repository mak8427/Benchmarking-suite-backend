import re
import jwt
from fastapi import HTTPException, Header
from main import SECRET


def current_user(authorization: str = Header(...)):
    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer": raise ValueError("bad scheme")
        payload = jwt.decode(token, SECRET, algorithms=["HS256"])
        return {"username": payload["sub"]}
    except Exception:
        raise HTTPException(status_code=401, detail="invalid or expired token")

def sanitize(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    if not name or ".." in name or name.startswith("/"):
        raise HTTPException(400, "invalid filename")
    return name
