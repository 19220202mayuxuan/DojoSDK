from __future__ import annotations

import time
import random
import platform
from typing import Any, Type, TypeVar, Mapping, cast
import anyio
import httpx
import pydantic

from dojo._compat import model_validate, model_construct, BaseModel
from dojo._exceptions import (
    DojoError,
    APIError,
    APIConnectionError,
    APITimeoutError,
    APIResponseValidationError,
    APIStatusError,
    BadRequestError,
    AuthenticationError,
    PermissionDeniedError,
    NotFoundError,
    ConflictError,
    UnprocessableEntityError,
    RateLimitError,
    InternalServerError,
)

from dojo.logging import logger as log

ResponseT = TypeVar("ResponseT")


class APIResponse:
    """Wrapper that holds both the parsed model and the raw HTTP response metadata."""

    def __init__(self, raw_response: httpx.Response, parsed_data: Any) -> None:
        self.raw_response = raw_response
        self.status_code = raw_response.status_code
        self.headers = raw_response.headers
        self.parsed_data = parsed_data


class StructuredOfflineResponse(httpx.Response):
    """HTTP-compatible metadata wrapper backed by an existing Python payload."""

    def __init__(self, *, payload: Any, request: httpx.Request) -> None:
        super().__init__(
            200,
            headers={"X-Dojo-Data-Source": "offline"},
            request=request,
        )
        self.structured_data = payload

    def json(self, **_kwargs: Any) -> Any:
        return self.structured_data


class BaseClient:
    def __init__(
        self,
        *,
        base_url: str,
        max_retries: int = 1,
        custom_headers: Mapping[str, str] | None = None,
        custom_query: Mapping[str, object] | None = None,
        return_raw_data: bool = True,
    ) -> None:
        self.base_url = httpx.URL(base_url)
        self.max_retries = max_retries
        self._custom_headers = custom_headers or {}
        self._custom_query = custom_query or {}
        self.return_raw_data = return_raw_data

    @property
    def online(self) -> bool:
        """Whether this client reads from the online QData API."""
        return bool(getattr(self, "_online", True))

    @property
    def default_headers(self) -> dict[str, str]:
        # Emits default headers including language and platform telemetry
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "DojoSDK/Python 0.1.5",
            "X-Dojo-Lang": "python",
            "X-Dojo-OS": platform.system().lower(),
            "X-Dojo-Arch": platform.machine().lower(),
        }
        headers.update(self._custom_headers)
        return headers

    def _check_anonymous_access(self, method: str, path: str) -> None:
        if getattr(self, "_anonymous", False) and (method.upper() != "GET" or path != "/api/qdata/v1/analysis/public_research"):
            raise DojoError("Anonymous clients only support GET /api/qdata/v1/analysis/public_research")

    def _prepare_url(self, path: str) -> httpx.URL:
        # Merge path with base url
        if path.startswith("/"):
            path = path.lstrip("/")
        return self.base_url.join(path)

    def _make_status_error(self, response: httpx.Response, body: Any) -> APIStatusError:
        code = response.status_code
        err_msg = f"Error code: {code}"
        if isinstance(body, dict) and "detail" in body:
            err_msg += f" - {body['detail']}"
        elif isinstance(body, dict) and "message" in body:
            err_msg += f" - {body['message']}"
        elif isinstance(body, str) and body:
            err_msg += f" - {body}"

        if code == 400:
            return BadRequestError(err_msg, response=response, body=body)
        elif code == 401:
            return AuthenticationError(err_msg, response=response, body=body)
        elif code == 403:
            return PermissionDeniedError(err_msg, response=response, body=body)
        elif code == 404:
            return NotFoundError(err_msg, response=response, body=body)
        elif code == 409:
            return ConflictError(err_msg, response=response, body=body)
        elif code == 422:
            return UnprocessableEntityError(err_msg, response=response, body=body)
        elif code == 429:
            return RateLimitError(err_msg, response=response, body=body)
        elif code >= 500:
            return InternalServerError(err_msg, response=response, body=body)
        return APIStatusError(err_msg, response=response, body=body)

    def _parse_retry_after(self, response: httpx.Response) -> float | None:
        headers = response.headers
        try:
            # Check non-standard ms header first
            if "retry-after-ms" in headers:
                return float(headers["retry-after-ms"]) / 1000.0
            if "retry-after" in headers:
                return float(headers["retry-after"])
        except (ValueError, TypeError):
            pass
        return None

    def _calculate_retry_delay(self, retry_count: int, response: httpx.Response | None = None) -> float:
        if response is not None:
            retry_after = self._parse_retry_after(response)
            if retry_after is not None and 0 < retry_after <= 60:
                return retry_after

        # Exponential backoff: delay = initial_delay * 2^retry * (1 - jitter)
        initial_delay = 0.5
        delay = initial_delay * (2.0**retry_count)
        jitter = 0.25 * random.random()
        return delay * (1 - jitter)

    def _should_retry(self, response: httpx.Response) -> bool:
        # Retry on rate limit (429) or server errors (500+)
        return response.status_code == 429 or response.status_code >= 500

    def _process_response(
        self,
        response: httpx.Response,
        cast_to: Type[ResponseT],
    ) -> ResponseT:
        # Check for non-200 status code
        if response.status_code != 200:
            try:
                body = response.json()
            except Exception:
                body = response.text
            raise self._make_status_error(response, body)

        try:
            data = response.json()
        except Exception as e:
            raise APIError(f"Failed to parse response JSON: {e}")

        return self._process_data(data, cast_to, response=response)

    def _process_data(
        self,
        data: Any,
        cast_to: Type[ResponseT],
        *,
        response: httpx.Response | None = None,
    ) -> ResponseT:
        """Parse an already-structured payload without a JSON encode/decode cycle."""

        if cast_to is None or cast_to is type(None):
            return cast(ResponseT, None)

        # Unwrap QData standard API envelope and check business errors
        payload = data
        if isinstance(data, dict) and "code" in data:
            code = data.get("code")
            if str(code) not in ("0", "200", "00000", "success"):
                msg = data.get("msg", data.get("message", f"API business error with code {code}"))
                error_response = response if response is not None else httpx.Response(200)
                raise APIStatusError(
                    f"Business Error: {msg} (code: {code})",
                    response=error_response,
                    body=data,
                )
            if "data" in data:
                payload = data["data"]

        if getattr(self, "return_raw_data", True):
            return cast(ResponseT, payload)

        if isinstance(cast_to, type) and issubclass(cast_to, BaseModel):

            # Inspect model fields
            if hasattr(cast_to, "model_fields"):
                fields = cast_to.model_fields
            else:
                fields = getattr(cast_to, "__fields__", {})

            # Map payload list to single list field
            if isinstance(payload, list):
                list_field = "data" if "data" in fields else next(iter(fields), None)
                if list_field:
                    payload = {list_field: payload}

            # Map payload dict with "data" to list fields
            elif isinstance(payload, dict):
                payload = dict(payload)
                if "data" in payload and "data" not in fields:
                    target_field = None
                    for field_name in fields:
                        if field_name not in [
                            "total_num",
                            "symbol",
                            "exchange",
                            "bz_type",
                        ]:
                            target_field = field_name
                            break
                    if target_field:
                        payload[target_field] = payload.pop("data")

                # Ensure default values for missing required fields to avoid validation errors
                for field_name in fields:
                    if field_name not in payload:
                        if field_name == "success":
                            payload[field_name] = True
                        else:
                            payload[field_name] = None

            try:
                return model_validate(cast_to, payload)
            except pydantic.ValidationError as err:
                log.warning(f"Validation failed for {cast_to.__name__}: {err}. Falling back to model construction.")
                if isinstance(payload, dict):
                    return model_construct(cast_to, **payload)
                raise APIResponseValidationError(
                    response=response if response is not None else httpx.Response(200),
                    body=data,
                ) from err

        return cast(ResponseT, data)


class SyncAPIClient(BaseClient):
    _client: httpx.Client

    def request(
        self,
        method: str,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        self._check_anonymous_access(method, path)
        options = options or {}
        params = {**self._custom_query, **options.get("params", {})}
        headers = {**self.default_headers, **options.get("headers", {})}
        json_data = options.get("json", None)
        timeout = options.get("timeout", httpx.USE_CLIENT_DEFAULT)
        is_raw = options.get("is_raw", False)

        if not getattr(self, "_online", True) and getattr(self, "_data_source", None) is not None:
            payload = self._data_source.fetch(
                method=method,
                path=path,
                params=params,
                json=json_data,
            )
            parsed_data = self._process_data(payload, cast_to)
            if is_raw:
                response = StructuredOfflineResponse(
                    payload=payload,
                    request=httpx.Request(method, self._prepare_url(path)),
                )
                return cast(ResponseT, APIResponse(response, parsed_data))
            return parsed_data

        url = self._prepare_url(path)
        request = self._client.build_request(
            method=method,
            url=url,
            params=params,
            headers=headers,
            json=json_data,
            timeout=timeout,
        )

        if getattr(self, "_anonymous", False):
            request.headers.pop("Authorization", None)
            request.headers.pop("Cookie", None)

        retries_remaining = options.get("max_retries", self.max_retries)
        retry_count = 0

        while True:
            try:
                response = self._client.send(request, auth=None) if getattr(self, "_anonymous", False) else self._client.send(request)
                if self._should_retry(response) and retries_remaining > 0:
                    delay = self._calculate_retry_delay(retry_count, response)
                    time.sleep(delay)
                    retries_remaining -= 1
                    retry_count += 1
                    continue
                break
            except httpx.TimeoutException as err:
                if retries_remaining > 0:
                    delay = self._calculate_retry_delay(retry_count)
                    time.sleep(delay)
                    retries_remaining -= 1
                    retry_count += 1
                    continue
                raise APITimeoutError(request=request) from err
            except httpx.RequestError as err:
                if retries_remaining > 0:
                    delay = self._calculate_retry_delay(retry_count)
                    time.sleep(delay)
                    retries_remaining -= 1
                    retry_count += 1
                    continue
                raise APIConnectionError(request=request) from err

        # Process response and cast
        # Check if raw response wrapper was requested
        is_raw = options.get("is_raw", False)
        parsed_data = self._process_response(response, cast_to)

        if is_raw:
            return cast(ResponseT, APIResponse(response, parsed_data))
        return parsed_data

    # Helper HTTP verb methods
    def get(
        self,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        return self.request("GET", path, cast_to=cast_to, options=options)

    def post(
        self,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        return self.request("POST", path, cast_to=cast_to, options=options)

    def put(
        self,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        return self.request("PUT", path, cast_to=cast_to, options=options)

    def delete(
        self,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        return self.request("DELETE", path, cast_to=cast_to, options=options)


class AsyncAPIClient(BaseClient):
    _client: httpx.AsyncClient

    async def request(
        self,
        method: str,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        self._check_anonymous_access(method, path)
        options = options or {}
        params = {**self._custom_query, **options.get("params", {})}
        headers = {**self.default_headers, **options.get("headers", {})}
        json_data = options.get("json", None)
        timeout = options.get("timeout", httpx.USE_CLIENT_DEFAULT)
        is_raw = options.get("is_raw", False)

        if not getattr(self, "_online", True) and getattr(self, "_data_source", None) is not None:
            import functools

            payload = await anyio.to_thread.run_sync(
                functools.partial(
                    self._data_source.fetch,
                    method=method,
                    path=path,
                    params=params,
                    json=json_data,
                )
            )
            parsed_data = self._process_data(payload, cast_to)
            if is_raw:
                response = StructuredOfflineResponse(
                    payload=payload,
                    request=httpx.Request(method, self._prepare_url(path)),
                )
                return cast(ResponseT, APIResponse(response, parsed_data))
            return parsed_data

        url = self._prepare_url(path)
        request = self._client.build_request(
            method=method,
            url=url,
            params=params,
            headers=headers,
            json=json_data,
            timeout=timeout,
        )

        if getattr(self, "_anonymous", False):
            request.headers.pop("Authorization", None)
            request.headers.pop("Cookie", None)

        retries_remaining = options.get("max_retries", self.max_retries)
        retry_count = 0

        while True:
            try:
                response = await self._client.send(request, auth=None) if getattr(self, "_anonymous", False) else await self._client.send(request)
                if self._should_retry(response) and retries_remaining > 0:
                    delay = self._calculate_retry_delay(retry_count, response)
                    await anyio.sleep(delay)
                    retries_remaining -= 1
                    retry_count += 1
                    continue
                break
            except httpx.TimeoutException as err:
                if retries_remaining > 0:
                    delay = self._calculate_retry_delay(retry_count)
                    await anyio.sleep(delay)
                    retries_remaining -= 1
                    retry_count += 1
                    continue
                raise APITimeoutError(request=request) from err
            except httpx.RequestError as err:
                if retries_remaining > 0:
                    delay = self._calculate_retry_delay(retry_count)
                    await anyio.sleep(delay)
                    retries_remaining -= 1
                    retry_count += 1
                    continue
                raise APIConnectionError(request=request) from err

        # Process response and cast
        is_raw = options.get("is_raw", False)
        parsed_data = self._process_response(response, cast_to)

        if is_raw:
            return cast(ResponseT, APIResponse(response, parsed_data))
        return parsed_data

    # Helper HTTP verb methods
    async def get(
        self,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        return await self.request("GET", path, cast_to=cast_to, options=options)

    async def post(
        self,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        return await self.request("POST", path, cast_to=cast_to, options=options)

    async def put(
        self,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        return await self.request("PUT", path, cast_to=cast_to, options=options)

    async def delete(
        self,
        path: str,
        *,
        cast_to: Type[ResponseT],
        options: dict[str, Any] | None = None,
    ) -> ResponseT:
        return await self.request("DELETE", path, cast_to=cast_to, options=options)
