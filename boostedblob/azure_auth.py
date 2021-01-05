from __future__ import annotations

import base64
import datetime
import hmac
import json
import os
import re
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Sequence, Tuple

import xmltodict

if TYPE_CHECKING:
    from .path import AzurePath
    from .request import Request


AZURE_SAS_TOKEN_EXPIRATION_SECONDS = 60 * 60
# these seem to be expired manually, but we don't currently detect that
AZURE_SHARED_KEY_EXPIRATION_SECONDS = 24 * 60 * 60
OAUTH_TOKEN = "oauth_token"
SHARED_KEY = "shared_key"


def load_credentials() -> Dict[str, Any]:
    # AZURE_STORAGE_KEY seems to be the environment variable mentioned by the az cli
    # AZURE_STORAGE_ACCOUNT_KEY is mentioned elsewhere on the internet
    for varname in ["AZURE_STORAGE_KEY", "AZURE_STORAGE_ACCOUNT_KEY"]:
        if varname in os.environ:
            result = dict(storageAccountKey=os.environ[varname])
            if "AZURE_STORAGE_ACCOUNT" in os.environ:
                result["account"] = os.environ["AZURE_STORAGE_ACCOUNT"]
            return result

    if "AZURE_APPLICATION_CREDENTIALS" in os.environ:
        creds_path = os.environ["AZURE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(creds_path):
            raise RuntimeError(
                f"Credentials not found at '{creds_path}' specified by environment variable "
                "'AZURE_APPLICATION_CREDENTIALS'"
            )
        with open(creds_path) as f:
            return json.load(f)

    if "AZURE_CLIENT_ID" in os.environ:
        return dict(
            appId=os.environ["AZURE_CLIENT_ID"],
            password=os.environ["AZURE_CLIENT_SECRET"],
            tenant=os.environ["AZURE_TENANT_ID"],
        )

    if "AZURE_STORAGE_CONNECTION_STRING" in os.environ:
        connection_data = {}
        # technically this should be parsed according to the rules in
        # https://www.connectionstrings.com/formating-rules-for-connection-strings/
        for part in os.environ["AZURE_STORAGE_CONNECTION_STRING"].split(";"):
            key, _, val = part.partition("=")
            connection_data[key.lower()] = val
        return dict(
            account=connection_data["accountname"], storageAccountKey=connection_data["accountkey"]
        )

    # look for a refresh token in the az command line credentials
    # https://mikhail.io/2019/07/how-azure-cli-manages-access-tokens/
    default_creds_path = os.path.expanduser("~/.azure/accessTokens.json")
    if os.path.exists(default_creds_path):
        with open(default_creds_path) as f:
            tokens = json.load(f)
            best_token = None
            for token in tokens:
                if best_token is None:
                    best_token = token
                else:
                    # expiresOn may be missing for tokens from service principals
                    if token.get("expiresOn", "") > best_token.get("expiresOn", ""):
                        best_token = token
            if best_token is not None:
                return best_token

    raise RuntimeError(
        """Azure credentials not found, please do one of the following:

1) Log in with 'az login', blobfile will use your default credentials to lookup your storage
   account key
2) Set the environment variable 'AZURE_STORAGE_KEY' to your storage account key which you can
   find by following this guide:
   https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage
3) Create an account with 'az ad sp create-for-rbac --name <name>' and set the
   'AZURE_APPLICATION_CREDENTIALS' environment variable to the path of the output from that
   command or individually set the 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET', and
   'AZURE_TENANT_ID' environment variables
"""
    )


def load_stored_subscription_ids() -> List[str]:
    """Return a list of subscription ids from the local azure profile.

    The default subscription will appear first in the list.

    """
    default_profile_path = os.path.expanduser("~/.azure/azureProfile.json")
    if not os.path.exists(default_profile_path):
        return []

    with open(default_profile_path, "rb") as f:
        # this file has a UTF-8 BOM
        profile = json.loads(f.read().decode("utf-8-sig"))

    subscriptions = profile["subscriptions"]
    subscriptions.sort(key=lambda x: x["isDefault"], reverse=True)
    return [sub["id"] for sub in subscriptions]


async def get_access_token(cache_key: Tuple[str, Optional[str]]) -> Tuple[Any, float]:
    account, container = cache_key

    now = time.time()
    creds = load_credentials()

    if "storageAccountKey" in creds:
        if "account" in creds:
            if creds["account"] != account:
                raise RuntimeError(
                    f"Provided storage account key for account '{creds['account']}' via "
                    f"environment variables, but needed credentials for account '{account}'"
                )
        auth = (SHARED_KEY, creds["storageAccountKey"])
        if await can_access_account(account, container, auth):
            return (auth, now + AZURE_SHARED_KEY_EXPIRATION_SECONDS)
        raise RuntimeError(
            f"Found storage account key, but it was unable to access storage account: '{account}'"
        )

    if "refreshToken" in creds:
        # we have a refresh token, convert it into an access token for this account
        req = create_access_token_request(
            creds=creds, scope=f"https://{account}.blob.core.windows.net/", success_codes=(200, 400)
        )

        async with req.execute() as resp:
            result = await resp.json()
            if resp.status == 400:
                if (
                    result["error"] == "invalid_grant"
                    and "AADSTS700082" in result["error_description"]
                ) or (
                    result["error"] == "interaction_required"
                    and "AADSTS50078" in result["error_description"]
                ):
                    raise RuntimeError(
                        "Your refresh token has expired, please run `az login` to refresh it"
                    )
                raise RuntimeError(
                    "Encountered an error when requesting an access token:\n"
                    f"{result['error']}: {result['error_description']}\n\n"
                    "Try running `az login`?"
                )

        auth = (OAUTH_TOKEN, result["access_token"])

        # for some azure accounts this access token does not work, check if it works
        if await can_access_account(account, container, auth):
            return (auth, now + float(result["expires_in"]))

        # it didn't work, fall back to getting the storage keys
        storage_account_key_auth = await get_storage_account_key(
            account=account, creds=creds, container_hint=container
        )
        if storage_account_key_auth is not None:
            return (storage_account_key_auth, now + AZURE_SHARED_KEY_EXPIRATION_SECONDS)

    if "appId" in creds:
        # we have a service principal, get an oauth token
        req = create_access_token_request(creds=creds, scope="https://storage.azure.com/")

        async with req.execute() as resp:
            result = await resp.json()
        auth = (OAUTH_TOKEN, result["access_token"])
        if await can_access_account(account, container, auth):
            return (auth, now + float(result["expires_in"]))

        # it didn't work, fall back to getting the storage keys
        storage_account_key_auth = await get_storage_account_key(
            account=account, creds=creds, container_hint=container
        )
        if storage_account_key_auth is not None:
            return (storage_account_key_auth, now + AZURE_SHARED_KEY_EXPIRATION_SECONDS)

    raise RuntimeError(
        f"Could not find any credentials that grant access to storage account: '{account}'"
    )


async def can_access_account(account: str, container: Optional[str], auth: Tuple[str, str]) -> bool:
    from .request import Request, azurify_request

    if not container:
        # if a container isn't specified, check that we can list the storage account
        req = Request(
            method="GET",
            url=f"https://{account}.blob.core.windows.net",
            params={"comp": "list", "maxresults": "1"},
            success_codes=(200, 403),
        )
        req = await azurify_request(req, auth=auth)

        async with req.execute() as resp:
            if resp.status == 403:
                return False
            data = await resp.read()

        out = xmltodict.parse(data)
        if out["EnumerationResults"]["Containers"] is None:
            # there are no containers in this storage account
            # we can't test if we can access this storage account or not, so presume we can
            return True

        # then also test that we can list a container. this is perhaps unnecessary...
        container = out["EnumerationResults"]["Containers"]["Container"]["Name"]

    # https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=list
    req = Request(
        method="GET",
        url=f"https://{account}.blob.core.windows.net/{container}",
        params={"restype": "container", "comp": "list", "maxresults": "1"},
        success_codes=(200, 403),
    )
    req = await azurify_request(req, auth=auth)
    async with req.execute() as resp:
        return resp.status == 200


def create_access_token_request(
    creds: Mapping[str, str], scope: str, success_codes: Sequence[int] = (200,)
) -> Request:
    from .request import Request

    if "refreshToken" in creds:
        # https://docs.microsoft.com/en-us/azure/active-directory/develop/v1-protocols-oauth-code#refreshing-the-access-tokens
        data = {
            "grant_type": "refresh_token",
            "refresh_token": creds["refreshToken"],
            "resource": scope,
        }
        tenant = "common"
    else:
        # https://docs.microsoft.com/en-us/azure/active-directory/develop/v1-oauth2-client-creds-grant-flow#request-an-access-token
        # https://docs.microsoft.com/en-us/azure/active-directory/develop/v1-protocols-oauth-code
        # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-azure-active-directory#use-oauth-access-tokens-for-authentication
        # https://docs.microsoft.com/en-us/rest/api/azure/
        # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-azure-active-directory
        # az ad sp create-for-rbac --name <name>
        # az account list
        # az role assignment create --role "Storage Blob Data Contributor" --assignee <appid> --scope "/subscriptions/<account id>"
        data = {
            "grant_type": "client_credentials",
            "client_id": creds["appId"],
            "client_secret": creds["password"],
            "resource": scope,
        }
        tenant = creds["tenant"]
    return Request(
        url=f"https://login.microsoftonline.com/{tenant}/oauth2/token",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=urllib.parse.urlencode(data).encode("utf8"),
        success_codes=success_codes,
    )


async def get_storage_account_id_with_subscription(
    subscription_id: str, account: str, auth: Tuple[str, str]
) -> Optional[str]:
    from .request import Request, azurify_request

    req = Request(
        method="GET",
        url=f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Storage/storageAccounts",
        params={"api-version": "2019-04-01"},
        success_codes=(200, 401, 403),
    )
    req = await azurify_request(req, auth=auth)

    async with req.execute() as resp:
        if resp.status in (401, 403):
            # we aren't allowed to query this for this subscription, skip it
            return None
        out = await resp.json()

    # search for the storage account
    return next((obj["id"] for obj in out["value"] if obj["name"] == account), None)


async def get_storage_account_id(account: str, auth: Tuple[str, str]) -> Optional[str]:
    from .request import Request, azurify_request

    stored_subscription_ids = load_stored_subscription_ids()
    for subscription_id in stored_subscription_ids:
        storage_account_id = await get_storage_account_id_with_subscription(
            subscription_id, account, auth
        )
        if storage_account_id:
            return storage_account_id

    req = Request(
        method="GET",
        url="https://management.azure.com/subscriptions",
        params={"api-version": "2020-01-01"},
    )
    req = await azurify_request(req, auth=auth)
    async with req.execute() as resp:
        result = await resp.json()
    subscription_ids = {item["subscriptionId"] for item in result["value"]}

    for subscription_id in subscription_ids - set(stored_subscription_ids):
        storage_account_id = await get_storage_account_id_with_subscription(
            subscription_id, account, auth
        )
        if storage_account_id:
            return storage_account_id
    return None


async def get_storage_account_key(
    account: str, creds: Mapping[str, Any], container_hint: Optional[str] = None
) -> Optional[Tuple[Any, float]]:
    from .request import Request, azurify_request

    # get an access token for the management service
    req = create_access_token_request(creds=creds, scope="https://management.azure.com/")

    async with req.execute() as resp:
        result = await resp.json()
    auth = (OAUTH_TOKEN, result["access_token"])

    storage_account_id = await get_storage_account_id(account, auth)
    if not storage_account_id:
        return None

    req = Request(
        method="POST",
        url=f"https://management.azure.com{storage_account_id}/listKeys",
        params={"api-version": "2019-04-01"},
    )
    req = await azurify_request(req, auth=auth)

    async with req.execute() as resp:
        result = await resp.json()
    for key in result["keys"]:
        if key["permissions"] == "FULL":
            storage_key_auth = (SHARED_KEY, key["value"])
            if await can_access_account(account, container_hint, storage_key_auth):
                return storage_key_auth
            raise RuntimeError(
                f"Found storage account key, but it was unable to access storage account: '{account}'"
            )
    raise RuntimeError(
        f"Storage account was found, but storage account keys were missing: '{account}'"
    )


def sign_request_with_shared_key(request: Request, key: str) -> str:
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
    from yarl import URL

    # Figuring out that shared key authorisation sometimes breaks because aiohttp is a little over
    # clever about canonicalising URLs was not fun; preemptively canonicalise
    parsed_url = urllib.parse.urlparse(str(URL(request.url)))
    storage_account = parsed_url.netloc.split(".")[0]
    blob = parsed_url.path[1:]

    def canonicalized_resource() -> str:
        params_to_sign = sorted(f"{name.lower()}:{value}" for name, value in request.params.items())
        canonical_url = f"/{storage_account}/{blob}"
        return "\n".join([canonical_url] + params_to_sign)

    def canonicalized_headers() -> str:
        headers_to_sign = []
        for name, value in request.headers.items():
            canonical_name = name.lower()
            canonical_value = re.sub(r"\s+", " ", value).strip()
            if canonical_name.startswith("x-ms-"):
                headers_to_sign.append(f"{canonical_name}:{canonical_value}")
        return "\n".join(sorted(headers_to_sign))

    headers = request.headers
    content_length = headers.get("Content-Length", "")
    if request.data is not None:
        content_length = str(len(request.data))

    parts_to_sign = [
        request.method,
        headers.get("Content-Encoding", ""),
        headers.get("Content-Language", ""),
        content_length,
        headers.get("Content-MD5", ""),
        headers.get("Content-Type", ""),
        headers.get("Date", ""),
        headers.get("If-Modified-Since", ""),
        headers.get("If-Match", ""),
        headers.get("If-None-Match", ""),
        headers.get("If-Unmodified-Since", ""),
        headers.get("Range", ""),
        canonicalized_headers(),
        canonicalized_resource(),
    ]
    string_to_sign = "\n".join(parts_to_sign)

    signature = base64.b64encode(
        hmac.digest(base64.b64decode(key), string_to_sign.encode("utf8"), "sha256")
    ).decode("utf8")

    return f"SharedKey {storage_account}:{signature}"


async def get_sas_token(cache_key: Tuple[str, Optional[str]]) -> Tuple[Any, float]:
    from .globals import config
    from .request import Request, azurify_request

    auth = await config.azure_access_token_manager.get_token(key=cache_key)
    account, container = cache_key

    if auth[0] != OAUTH_TOKEN:
        cmd = (
            f"az storage container list --auth-mode login --account-name {account}"
            if container is None
            else f"az storage blob list --auth-mode login --account-name {account} --container {container}"
        )
        raise RuntimeError(
            "Only OAuth tokens can be used to get SAS tokens. You should set the "
            "Blob Data Reader or Storage Blob Data Contributor IAM role. You can run "
            f"`{cmd}` to confirm that the missing role is the issue."
        )

    # https://docs.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas
    now = datetime.datetime.utcnow()
    start = (now + datetime.timedelta(hours=-1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    expiration = now + datetime.timedelta(days=6)
    expiry = expiration.strftime("%Y-%m-%dT%H:%M:%SZ")
    req = await azurify_request(
        Request(
            url=f"https://{account}.blob.core.windows.net/",
            method="POST",
            params=dict(restype="service", comp="userdelegationkey"),
            data={"KeyInfo": {"Start": start, "Expiry": expiry}},
            success_codes=(200, 403),
        ),
        auth=auth,
    )
    async with req.execute() as resp:
        if resp.status == 403:
            raise RuntimeError(
                f"You do not have permission to generate an SAS token for account {account}. "
                "Try setting the Storage Blob Delegator or Storage Blob Data Contributor IAM role "
                "at the account level."
            )
        data = await resp.read()

    out = xmltodict.parse(data)
    return out["UserDelegationKey"], time.time() + AZURE_SAS_TOKEN_EXPIRATION_SECONDS


async def generate_signed_url(path: AzurePath) -> Tuple[str, datetime.datetime]:
    # https://docs.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature
    # https://docs.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas
    # https://docs.microsoft.com/en-us/rest/api/storageservices/service-sas-examples
    from .globals import config

    key = await config.azure_sas_token_manager.get_token(key=(path.account, path.container or None))
    params = {
        "st": key["SignedStart"],
        "se": key["SignedExpiry"],
        "sks": key["SignedService"],
        "skt": key["SignedStart"],
        "ske": key["SignedExpiry"],
        "sktid": key["SignedTid"],
        "skoid": key["SignedOid"],
        # signed key version (param name not mentioned in docs)
        "skv": key["SignedVersion"],
        "sv": "2018-11-09",  # signed version
        "sr": "b",  # signed resource
        "sp": "r",  # signed permissions
        "sip": "",  # signed ip
        "si": "",  # signed identifier
        "spr": "https,http",  # signed http protocol
        "rscc": "",  # Cache-Control header
        "rscd": "",  # Content-Disposition header
        "rsce": "",  # Content-Encoding header
        "rscl": "",  # Content-Language header
        "rsct": "",  # Content-Type header
    }

    canonicalized_resource = f"/blob/{path.account}/{path.container}/{path.blob}"
    parts_to_sign = (
        params["sp"],
        params["st"],
        params["se"],
        canonicalized_resource,
        params["skoid"],
        params["sktid"],
        params["skt"],
        params["ske"],
        params["sks"],
        params["skv"],
        params["sip"],
        params["spr"],
        params["sv"],
        params["sr"],
        params["rscc"],
        params["rscd"],
        params["rsce"],
        params["rscl"],
        params["rsct"],
        # this is documented on a different page
        # https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas#specifying-the-signed-identifier
        params["si"],
    )
    string_to_sign = "\n".join(parts_to_sign)
    params["sig"] = base64.b64encode(
        hmac.digest(base64.b64decode(key["Value"]), string_to_sign.encode("utf8"), "sha256")
    ).decode("utf8")
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v != ""})

    expiry = datetime.datetime.strptime(key["SignedExpiry"], "%Y-%m-%dT%H:%M:%SZ")
    url = path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}")
    return url + "?" + query, expiry
