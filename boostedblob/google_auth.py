from __future__ import annotations

import base64
import binascii
import dataclasses
import datetime
import hashlib
import json
import os
import socket
import sys
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, List, Mapping, Tuple

if TYPE_CHECKING:
    from .path import GooglePath
    from .request import Request

MAX_EXPIRATION_SECONDS = 7 * 24 * 60 * 60


def default_gcloud_path() -> str:
    if sys.platform == "win32":
        # https://www.jhanley.com/google-cloud-application-default-credentials/
        return os.path.join(os.environ["APPDATA"], "gcloud")
    return os.path.join(os.environ["HOME"], ".config/gcloud")


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

    default_creds_path = os.path.join(default_gcloud_path(), "application_default_credentials.json")
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
        creds = load_credentials()
    except RuntimeError:
        if os.environ.get("NO_GCE_CHECK", "false").lower() != "true" and _is_gce_instance():
            # see if the metadata server has a token for us
            req = Request(
                method="GET",
                url="http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
                headers={"Metadata-Flavor": "Google"},
                auth=None,
            )
            async with req.execute() as resp:
                result = await resp.json()
            return result["access_token"], now + float(result["expires_in"])
        raise

    req = create_access_token_request(
        creds, scopes=["https://www.googleapis.com/auth/devstorage.full_control"]
    )
    req = dataclasses.replace(req, success_codes=(200, 400))

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


def create_access_token_request(creds: Mapping[str, Any], scopes: List[str]) -> Request:
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
        auth=None,
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
        auth=None,
    )


def _is_gce_instance() -> bool:
    try:
        socket.getaddrinfo("metadata.google.internal", 80)
    except socket.gaierror:
        return False
    return True


def _sign(private_key: str, msg: bytes) -> bytes:
    from Cryptodome.Hash import SHA256
    from Cryptodome.PublicKey import RSA
    from Cryptodome.Signature import pkcs1_15

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


def generate_signed_url(path: GooglePath) -> Tuple[str, datetime.datetime]:
    # https://cloud.google.com/storage/docs/access-control/signing-urls-manually
    creds = load_credentials()
    if "private_key" not in creds:
        raise RuntimeError(
            "Private key not found in credentials.  Please set the "
            "`GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to a JSON key for "
            "a service account to use this call"
        )

    host = "storage.googleapis.com"
    canonical_uri = path.format_url("/{bucket}/{blob}")

    now_local = datetime.datetime.now()
    now = datetime.datetime.now(datetime.timezone.utc)
    request_timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")

    credential_scope = f"{datestamp}/auto/storage/goog4_request"
    credential = f"{creds['client_email']}/{credential_scope}"

    canonical_headers = ""
    ordered_headers = sorted({"host": host}.items())
    for k, v in ordered_headers:
        lower_k = str(k).lower()
        strip_v = str(v).lower()
        canonical_headers += f"{lower_k}:{strip_v}\n"

    signed_headers_parts = []
    for k, _ in ordered_headers:
        lower_k = str(k).lower()
        signed_headers_parts.append(lower_k)
    signed_headers = ";".join(signed_headers_parts)

    params = {
        "X-Goog-Algorithm": "GOOG4-RSA-SHA256",
        "X-Goog-Credential": credential,
        "X-Goog-Date": request_timestamp,
        "X-Goog-Expires": str(MAX_EXPIRATION_SECONDS),
        "X-Goog-SignedHeaders": signed_headers,
    }

    canonical_query_string_parts = []
    ordered_params = sorted(params.items())
    for k, v in ordered_params:
        encoded_k = urllib.parse.quote(str(k), safe="")
        encoded_v = urllib.parse.quote(str(v), safe="")
        canonical_query_string_parts.append(f"{encoded_k}={encoded_v}")
    canonical_query_string = "&".join(canonical_query_string_parts)

    canonical_request = "\n".join(
        [
            "GET",
            canonical_uri,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            "UNSIGNED-PAYLOAD",
        ]
    )
    canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

    string_to_sign = "\n".join(
        ["GOOG4-RSA-SHA256", request_timestamp, credential_scope, canonical_request_hash]
    ).encode("utf8")
    signature = binascii.hexlify(_sign(creds["private_key"], string_to_sign)).decode("utf8")
    signed_url = f"https://{host}{canonical_uri}"
    signed_url += f"?{canonical_query_string}&X-Goog-Signature={signature}"
    return signed_url, now_local + datetime.timedelta(seconds=MAX_EXPIRATION_SECONDS)
