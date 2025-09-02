import time, jwt, secrets
from dataclasses import Field

from fastapi import FastAPI, HTTPException, status
from passlib.hash import argon2
from pydantic import BaseModel, Field
app=FastAPI()
SECRET=b"..."
TOKENS={}
USERS={"alice":argon2.hash("pass")}
app = FastAPI()

class UserCreate(BaseModel):
    username: str = Field(..., min_length=5, max_length=20)
    password: str = Field(..., min_length=8, max_length=128)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def register(payload: UserCreate):
    if payload.username in USERS:
        raise HTTPException(status_code=409, detail="Username already registered")

    USERS[payload.username]=argon2.hash(payload.password)
    rid = secrets.token_urlsafe(32)
    TOKENS[rid]={"sub":payload.username,"exp":time.time()+30*86400}

    #Save user to plain text, for now
    with open("users.txt","a") as f:
        f.write(f"{payload.username}:{argon2.hash(payload.password)}\n")
        f.close()
    return {"access":make_access(payload.username), "refresh":rid}



@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}



def make_access(sub):
    return jwt.encode({"sub":sub,"scope":"upload","exp":time.time()+600}, SECRET, "HS256")

@app.post("/auth/password")
def login(u:str,p:str):
    #load from plain text
    with open("users.txt","r") as f:
        for line in f:
            user,hash=line.split(":")
            USERS[user]=hash.strip()
        f.close()
    if u not in USERS or not argon2.verify(p, USERS[u]):
        raise HTTPException(401)
    rid=secrets.token_urlsafe(32)
    TOKENS[rid]= {"sub":u,"exp":time.time()+30*86400}
    return {"access":make_access(u), "refresh":rid}

@app.post("/auth/refresh")
def refresh(rid:str):
    t=TOKENS.get(rid);
    if not t or t["exp"]<time.time():
        raise HTTPException(401)
    new=secrets.token_urlsafe(32)
    TOKENS[new]=t
    TOKENS.pop(rid,None)
    return {"access":make_access(t["sub"]), "refresh":new}
