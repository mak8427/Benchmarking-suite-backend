import time

import jwt
import pytest
from fastapi import HTTPException

from util.auth_utils import current_user, sanitize
import main


def test_sanitize_allows_safe_names():
    assert sanitize("report.csv") == "report.csv"
    assert sanitize("folder-1.data") == "folder-1.data"


def test_sanitize_rejects_or_normalizes_bad_names():
    with pytest.raises(HTTPException):
        sanitize("../escape.txt")
    with pytest.raises(HTTPException):
        sanitize("")
    assert sanitize("invalid name.txt") == "invalid_name.txt"
    assert sanitize("semi;colon.txt") == "semi_colon.txt"


def test_current_user_requires_header():
    with pytest.raises(HTTPException):
        current_user()


def test_current_user_rejects_bad_scheme():
    token = jwt.encode({"sub": "bob", "exp": time.time() + 60}, main.SECRET, algorithm="HS256")
    with pytest.raises(HTTPException):
        current_user(authorization=f"Token {token}")


def test_current_user_rejects_invalid_token():
    with pytest.raises(HTTPException):
        current_user(authorization="Bearer invalidtoken")


def test_current_user_decodes_valid_token():
    token = jwt.encode({"sub": "bob", "exp": time.time() + 60}, main.SECRET, algorithm="HS256")
    user = current_user(authorization=f"Bearer {token}")
    assert user == {"username": "bob"}
