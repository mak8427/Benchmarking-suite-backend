import os, re
from fastapi import Depends, HTTPException
from minio import Minio
from jose import jwt, JWTError

MINIO = Minio(os.getenv("MINIO_ENDPOINT"),
              access_key=os.getenv("MINIO_ACCESS_KEY"),
              secret_key=os.getenv("MINIO_SECRET_KEY"), secure=False)
BUCKET = os.getenv("MINIO_BUCKET")