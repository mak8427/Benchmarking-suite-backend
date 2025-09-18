import time

import pytest

import main


def register(client, username="user123", password="strongPASS1"):
    return client.post("/auth/register", json={"username": username, "password": password})


def login(client, username="user123", password="strongPASS1"):
    return client.post("/auth/password", params={"u": username, "p": password})


def test_register_creates_user_and_tokens(client):
    response = register(client)
    assert response.status_code == 201
    data = response.json()
    assert "access" in data
    assert "refresh" in data
    assert "user123" in main.USERS
    assert any(token["sub"] == "user123" for token in main.TOKENS.values())
    with main.USERS_FILE.open("r", encoding="utf-8") as handle:
        contents = handle.read()
    assert "user123" in contents


def test_register_rejects_duplicates(client):
    first = register(client)
    assert first.status_code == 201
    duplicate = register(client)
    assert duplicate.status_code == 409


def test_login_returns_tokens_for_valid_credentials(client):
    register(client)
    response = login(client)
    assert response.status_code == 200
    body = response.json()
    assert "access" in body
    assert "refresh" in body
    assert any(entry["sub"] == "user123" for entry in main.TOKENS.values())


def test_login_rejects_unknown_user(client):
    response = login(client, username="nobody", password="nope1234")
    assert response.status_code == 401


def test_login_rejects_bad_password(client):
    register(client)
    response = login(client, password="wrongpass1")
    assert response.status_code == 401


def test_refresh_issues_new_tokens(client):
    register_response = register(client)
    refresh_token = register_response.json()["refresh"]
    response = client.post("/auth/refresh", params={"rid": refresh_token})
    assert response.status_code == 200
    body = response.json()
    assert body["refresh"] != refresh_token
    assert body["access"]


def test_refresh_rejects_unknown_token(client):
    response = client.post("/auth/refresh", params={"rid": "does-not-exist"})
    assert response.status_code == 401


def test_refresh_rejects_expired_token(client):
    rid = "expired-token"
    main.TOKENS[rid] = {"sub": "user123", "exp": time.time() - 10}
    response = client.post("/auth/refresh", params={"rid": rid})
    assert response.status_code == 401
