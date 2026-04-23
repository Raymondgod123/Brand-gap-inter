from __future__ import annotations

from dataclasses import dataclass
import ssl
import urllib.error
import urllib.request


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    final_url: str
    headers: dict[str, str]
    body: str


class HttpFetcher:
    def fetch(self, url: str, headers: dict[str, str] | None = None, timeout_seconds: int = 30) -> HttpResponse:
        raise NotImplementedError


class HttpFetchError(RuntimeError):
    pass


class UrllibHttpFetcher(HttpFetcher):
    def fetch(self, url: str, headers: dict[str, str] | None = None, timeout_seconds: int = 30) -> HttpResponse:
        request = urllib.request.Request(url, headers=headers or {})
        context = ssl.create_default_context()

        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
                body = response.read().decode("utf-8", "ignore")
                return HttpResponse(
                    status_code=response.status,
                    final_url=response.geturl(),
                    headers=dict(response.headers.items()),
                    body=body,
                )
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", "ignore")
            return HttpResponse(
                status_code=error.code,
                final_url=error.geturl(),
                headers=dict(error.headers.items()),
                body=body,
            )
        except Exception as error:
            raise HttpFetchError(str(error)) from error
