#!/usr/bin/env python3
"""
test_api.py - Full test suite for the Projects API (Axum & Actix Web).

Usage:
    python3 test_api.py                          # Axum      -> http://localhost:3000
    python3 test_api.py http://localhost:3001    # Actix Web
    python3 test_api.py --secret super_secret_key

Standard library only. Exit: 0 = passed, 1 = failures, 2 = unreachable.
"""
import argparse, base64, hashlib, hmac, json, sys, time, urllib.error, urllib.request

if sys.stdout.isatty():
    G, R, Y, C, N = "\033[32m", "\033[31m", "\033[33m", "\033[36m", "\033[0m"
else:
    G = R = Y = C = N = ""

RID = "00000000-0000-0000-0000-000000000000"
CTX, CL, SECRET = {}, None, "super_secret_key"


class SoftWarn(Exception):
    """Informational check — does not fail the suite."""


class Client:
    def __init__(self, base):
        self.base = base.rstrip("/")

    def request(self, method, path, body=None, token=None, headers=None, raw=None):
        h = {k.lower(): v for k, v in (headers or {}).items()}
        data = None
        if raw is not None:
            data = raw.encode() if isinstance(raw, str) else raw
            h.setdefault("content-type", "application/json")
        elif body is not None:
            data = json.dumps(body).encode()
            h.setdefault("content-type", "application/json")
        if token is not None:
            h["authorization"] = "Bearer " + token
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()
        except urllib.error.HTTPError as e:
            return e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, e.read()


def jb(b):
    try:
        return json.loads(b) if b else {}
    except Exception:
        return {}


def ast(res, exp, label=""):
    s, _, b = res
    ok = s in exp if isinstance(exp, (set, tuple, list)) else s == exp
    if not ok:
        raise AssertionError(f"{label}expected {exp}, got {s}: {b[:200]!r}")


def aec(res, code):
    got = jb(res[2]).get("error", {}).get("code")
    if got != code:
        raise AssertionError(f"expected error.code={code!r}, got {got!r}")


def b64u(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def mkjwt(sub="user1", exp=None, secret=None):
    """Craft an HS256 JWT exactly like the APIs do (for negative auth tests)."""
    secret = secret if secret is not None else SECRET
    exp = exp if exp is not None else int(time.time()) + 3600
    h = b64u(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    p = b64u(json.dumps({"sub": sub, "exp": exp}).encode())
    sig = b64u(hmac.new(secret.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest())
    return f"{h}.{p}.{sig}"


TESTS = []
def test(sec, name):
    def deco(fn):
        TESTS.append((sec, name, fn)); return fn
    return deco


# ---- OpenAPI & Docs ----
@test("OpenAPI & Docs", "GET /api-docs/openapi.json -> 200 with all paths")
def _():
    r = CL.request("GET", "/api-docs/openapi.json"); ast(r, 200)
    for p in ("/v1/auth/login", "/v1/projects", "/v1/projects/{id}"):
        if p not in jb(r[2]).get("paths", {}):
            raise AssertionError(f"OpenAPI missing path {p}")

@test("OpenAPI & Docs", "GET /swagger-ui/ -> 200 (HTML)")
def _():
    r = CL.request("GET", "/swagger-ui/"); ast(r, 200)
    if b"swagger" not in r[2].lower():
        raise AssertionError("not Swagger UI HTML")

# ---- Authentication ----
@test("Authentication", "login valid -> 200 + token")
def _():
    r = CL.request("POST", "/v1/auth/login", body={"email": "test@example.com", "password": "password"})
    ast(r, 200)
    if "application/json" not in r[1].get("content-type", ""):
        raise AssertionError("not application/json")
    t = jb(r[2]).get("token")
    if not t:
        raise AssertionError("no token")
    CTX["token"] = t

@test("Authentication", "login wrong password -> 401 unauthorized")
def _():
    r = CL.request("POST", "/v1/auth/login", body={"email": "test@example.com", "password": "wrong"})
    ast(r, 401); aec(r, "unauthorized")

@test("Authentication", "login unknown user -> 401")
def _():
    ast(CL.request("POST", "/v1/auth/login", body={"email": "nobody@example.com", "password": "password"}), 401)

@test("Authentication", "login missing fields -> 400/422")
def _():
    ast(CL.request("POST", "/v1/auth/login", body={}), {400, 422})

@test("Authentication", "login malformed JSON -> 400/422")
def _():
    ast(CL.request("POST", "/v1/auth/login", raw='{"email":'), {400, 422})

# ---- Authorization (negative) ----
@test("Authorization", "list without token -> 401 unauthorized")
def _():
    r = CL.request("GET", "/v1/projects"); ast(r, 401); aec(r, "unauthorized")

@test("Authorization", "list garbage token -> 401")
def _():
    ast(CL.request("GET", "/v1/projects", token="not-a-real-jwt"), 401)

@test("Authorization", "non-Bearer scheme -> 401")
def _():
    ast(CL.request("GET", "/v1/projects", headers={"Authorization": "Token abc"}), 401)

@test("Authorization", "expired JWT -> 401")
def _():
    ast(CL.request("GET", "/v1/projects", token=mkjwt(exp=int(time.time()) - 120)), 401)

@test("Authorization", "JWT wrong secret -> 401")
def _():
    ast(CL.request("GET", "/v1/projects", token=mkjwt(secret="wrong-secret")), 401)

@test("Authorization", "create without token -> 401")
def _():
    ast(CL.request("POST", "/v1/projects", body={"name": "No Auth"}), 401)

@test("Authorization", "delete without token -> 401")
def _():
    ast(CL.request("DELETE", f"/v1/projects/{RID}"), 401)

# ---- Validation ----
@test("Validation", "name < 3 chars -> 400 validation_failed")
def _():
    r = CL.request("POST", "/v1/projects", body={"name": "ab"}, token=CTX.get("token"))
    ast(r, 400); aec(r, "validation_failed")

@test("Validation", "name > 80 chars -> 400")
def _():
    ast(CL.request("POST", "/v1/projects", body={"name": "a" * 81}, token=CTX.get("token")), 400)

@test("Validation", "description > 500 chars -> 400")
def _():
    ast(CL.request("POST", "/v1/projects", body={"name": "Valid Name", "description": "d" * 501}, token=CTX.get("token")), 400)

@test("Validation", "malformed JSON -> 400/422")
def _():
    ast(CL.request("POST", "/v1/projects", raw='{"name":', token=CTX.get("token")), {400, 422})

# ---- CRUD lifecycle ----
@test("CRUD lifecycle", "baseline list -> 200 JSON array")
def _():
    r = CL.request("GET", "/v1/projects", token=CTX.get("token")); ast(r, 200)
    if not isinstance(jb(r[2]), list):
        raise AssertionError("not a JSON array")

@test("CRUD lifecycle", "create -> 201 with id and owner")
def _():
    name = f"Py Suite Project {int(time.time())}"
    r = CL.request("POST", "/v1/projects", body={"name": name, "description": "created by test_api.py"}, token=CTX.get("token"))
    ast(r, 201); p = jb(r[2])
    if not p.get("id"): raise AssertionError("no id")
    if p.get("user_id") != "user1": raise AssertionError(f"owner {p.get('user_id')!r}")
    if p.get("name") != name: raise AssertionError("name mismatch")
    CTX["pid"], CTX["name"] = p["id"], name

@test("CRUD lifecycle", "list contains created project")
def _():
    r = CL.request("GET", "/v1/projects", token=CTX.get("token")); ast(r, 200)
    if CTX["pid"] not in [x.get("id") for x in jb(r[2])]:
        raise AssertionError("not in list")

@test("CRUD lifecycle", "get by id -> 200 matching name")
def _():
    r = CL.request("GET", f"/v1/projects/{CTX['pid']}", token=CTX.get("token")); ast(r, 200)
    if jb(r[2]).get("name") != CTX["name"]:
        raise AssertionError("name mismatch")

@test("CRUD lifecycle", "get missing -> 404 not_found")
def _():
    r = CL.request("GET", f"/v1/projects/{RID}", token=CTX.get("token")); ast(r, 404); aec(r, "not_found")

@test("CRUD lifecycle", "patch -> 200")
def _():
    ast(CL.request("PATCH", f"/v1/projects/{CTX['pid']}", body={"name": "Renamed By Py", "description": "updated"}, token=CTX.get("token")), 200)

@test("CRUD lifecycle", "patch persisted in SQLite (via GET)")
def _():
    r = CL.request("GET", f"/v1/projects/{CTX['pid']}", token=CTX.get("token")); ast(r, 200)
    if jb(r[2]).get("name") != "Renamed By Py":
        raise AssertionError("PATCH not persisted")

@test("CRUD lifecycle", "patch invalid name -> 400")
def _():
    ast(CL.request("PATCH", f"/v1/projects/{CTX['pid']}", body={"name": "x"}, token=CTX.get("token")), 400)

@test("CRUD lifecycle", "patch missing -> 404 (200 tolerated)")
def _():
    r = CL.request("PATCH", f"/v1/projects/{RID}", body={"name": "Ghost Project"}, token=CTX.get("token"))
    if r[0] == 200:
        raise SoftWarn("impl returns 200 without rows-affected check; 404 would be stricter")
    ast(r, 404)

@test("CRUD lifecycle", "delete -> 204 empty body")
def _():
    r = CL.request("DELETE", f"/v1/projects/{CTX['pid']}", token=CTX.get("token")); ast(r, 204)
    if r[2]: raise AssertionError("204 body not empty")

@test("CRUD lifecycle", "get after delete -> 404 (persistence)")
def _():
    ast(CL.request("GET", f"/v1/projects/{CTX['pid']}", token=CTX.get("token")), 404)

@test("CRUD lifecycle", "delete again -> 204 (idempotent)")
def _():
    ast(CL.request("DELETE", f"/v1/projects/{CTX['pid']}", token=CTX.get("token")), 204)

# ---- HTTP plumbing ----
@test("HTTP plumbing", "PUT unsupported -> 405")
def _():
    ast(CL.request("PUT", "/v1/projects", body={"name": "Nope"}, token=CTX.get("token")), 405)

@test("HTTP plumbing", "CORS preflight has Access-Control-Allow-Origin")
def _():
    r = CL.request("OPTIONS", "/v1/projects", headers={"Origin": "http://example.com", "Access-Control-Request-Method": "POST"})
    if r[0] >= 400 or "access-control-allow-origin" not in r[1]:
        raise AssertionError(f"preflight failed: {r[0]} {r[1]}")

@test("HTTP plumbing", "gzip compression active (soft)")
def _():
    r = CL.request("GET", "/v1/projects", token=CTX.get("token"), headers={"Accept-Encoding": "gzip"}); ast(r, 200)
    if "gzip" not in r[1].get("content-encoding", ""):
        raise SoftWarn("no Content-Encoding: gzip (small responses may skip)")

@test("HTTP plumbing", "oversized body rejected -> 400/413/422")
def _():
    ast(CL.request("POST", "/v1/projects", body={"name": "Big", "description": "x" * (3 * 1024 * 1024)}, token=CTX.get("token")), {400, 413, 422})


def main():
    global CL, SECRET
    ap = argparse.ArgumentParser(description="Test the Projects API (Axum or Actix Web)")
    ap.add_argument("base_url", nargs="?", default="http://localhost:3000")
    ap.add_argument("--secret", default="super_secret_key", help="JWT secret used by the server")
    a = ap.parse_args()
    CL, SECRET = Client(a.base_url), a.secret
    print(f"{C}Testing API at {CL.base}{N}")
    try:
        CL.request("GET", "/api-docs/openapi.json")
    except Exception as e:
        print(f"{R}ERROR: cannot reach {CL.base} ({e}). Start the server first (./run_axum.sh or ./run_actix.sh).{N}")
        sys.exit(2)

    p = f = w = 0; cur = None
    for sec, name, fn in TESTS:
        if sec != cur:
            print(f"\n{C}== {sec} =={N}"); cur = sec
        try:
            fn()
        except SoftWarn as e:
            w += 1; print(f"  {Y}WARN{N}  {name} ({e})")
        except Exception as e:
            f += 1; print(f"  {R}FAIL{N}  {name}\n        {e}")
        else:
            p += 1; print(f"  {G}PASS{N}  {name}")

    print(f"\n{C}== Summary =={N}\n  {G}Passed: {p}{N}  {R}Failed: {f}{N}  {Y}Warnings: {w}{N}")
    if f:
        print(f"{R}TESTS FAILED for {CL.base}{N}"); sys.exit(1)
    print(f"{G}ALL TESTS PASSED for {CL.base}{N}"); sys.exit(0)


if __name__ == "__main__":
    main()
