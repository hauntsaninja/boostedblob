from __future__ import annotations

import base64
import json
import os
import socket
import sys
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, List, Mapping, Tuple

from Cryptodome.Hash import SHA256
from Cryptodome.PublicKey import RSA
from Cryptodome.Signature import pkcs1_15

if TYPE_CHECKING:
    from .request import Request


def load_credentials() -> Mapping[str, Any]:
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(creds_path):
            raise RuntimeError(
                f"Credentials not found at {creds_path} specified by environment variable "
                "'GOOGLE_APPLICATION_CREDENTIALS'"
            )
        with open(creds_path) as f:
            return json.load(f)
    if sys.platform == "win32":
        # https://www.jhanley.com/google-cloud-application-default-credentials/
        default_creds_path = os.path.join(
            os.environ["APPDATA"], "gcloud/application_default_credentials.json"
        )
    else:
        default_creds_path = os.path.join(
            os.environ["HOME"], ".config/gcloud/application_default_credentials.json"
        )

    if os.path.exists(default_creds_path):
        with open(default_creds_path) as f:
            return json.load(f)
    raise RuntimeError(
        "Credentials not found, please login with 'gcloud auth application-default login' or "
        "else set the 'GOOGLE_APPLICATION_CREDENTIALS' environment variable to the path of a "
        "JSON format service account key"
    )


async def get_access_token(_: str) -> Tuple[Any, float]:
    from .request import Request

    now = time.time()

    # https://github.com/googleapis/google-auth-library-java/blob/master/README.md#application-default-credentials
    try:
        load_credentials()
    except RuntimeError:
        if os.environ.get("NO_GCE_CHECK", "false").lower() != "true" and _is_gce_instance():
            # see if the metadata server has a token for us
            req = Request(
                method="GET",
                url="http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
                headers={"Metadata-Flavor": "Google"},
            )
            async with req.execute() as resp:
                result = await resp.json()
            return result["access_token"], now + float(result["expires_in"])
        raise

    req = create_access_token_request(
        scopes=["https://www.googleapis.com/auth/devstorage.full_control"]
    )
    req.success_codes = (200, 400)

    async with req.execute() as resp:
        result = await resp.json()
        if resp.status == 400:
            error = result["error"]
            description = result.get("error_description", "<missing description>")
            msg = f"Error with google credentials: [{error}] {description}"
            if error == "invalid_grant":
                if description.startswith("Invalid JWT:"):
                    msg += "\nPlease verify that your system clock is correct."
                elif description == "Bad Request":
                    msg += (
                        "\nYour credentials may be expired, please run the following commands: "
                        "`gcloud auth application-default revoke` (this may fail but ignore the "
                        "error) then `gcloud auth application-default login`"
                    )
            raise RuntimeError(msg)
        assert resp.status == 200
    return result["access_token"], now + float(result["expires_in"])


def create_access_token_request(scopes: List[str]) -> Request:
    creds = load_credentials()
    if "private_key" in creds:
        # looks like GCS does not support the no-oauth flow
        # https://developers.google.com/identity/protocols/OAuth2ServiceAccount#jwt-auth
        return create_token_request(creds["client_email"], creds["private_key"], scopes)
    if "refresh_token" in creds:
        return _refresh_access_token_request(
            refresh_token=creds["refresh_token"],
            client_id=creds["client_id"],
            client_secret=creds["client_secret"],
        )
    raise RuntimeError("Credentials not recognized")


def create_token_request(client_email: str, private_key: str, scopes: List[str]) -> Request:
    from .request import Request

    # https://developers.google.com/identity/protocols/OAuth2ServiceAccount
    now = time.time()
    claim_set = {
        "iss": client_email,
        "scope": " ".join(scopes),
        "aud": "https://www.googleapis.com/oauth2/v4/token",
        "exp": now + 60 * 60,
        "iat": now,
    }
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": _create_jwt(private_key, claim_set),
    }
    return Request(
        url="https://www.googleapis.com/oauth2/v4/token",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=urllib.parse.urlencode(data).encode("utf8"),
    )


def _refresh_access_token_request(
    client_id: str, client_secret: str, refresh_token: str
) -> Request:
    from .request import Request

    # https://developers.google.com/identity/protocols/OAuth2WebServer#offline
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    return Request(
        url="https://www.googleapis.com/oauth2/v4/token",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=urllib.parse.urlencode(data).encode("utf8"),
    )


def _is_gce_instance() -> bool:
    try:
        socket.getaddrinfo("metadata.google.internal", 80)
    except socket.gaierror:
        return False
    return True


def _sign(private_key: str, msg: bytes) -> bytes:
    key = RSA.import_key(private_key)
    h = SHA256.new(msg)
    return pkcs1_15.new(key).sign(h)


def _create_jwt(private_key: str, data: Mapping[str, Any]) -> bytes:
    encode = base64.urlsafe_b64encode
    header_b64 = encode(json.dumps({"alg": "RS256", "typ": "JWT"}).encode("utf8"))
    body_b64 = encode(json.dumps(data).encode("utf8"))
    to_sign = header_b64 + b"." + body_b64
    signature_b64 = encode(_sign(private_key, to_sign))
    return header_b64 + b"." + body_b64 + b"." + signature_b64
