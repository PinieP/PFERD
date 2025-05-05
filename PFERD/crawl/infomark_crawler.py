from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin


from PFERD.auth.authenticator import Authenticator
from PFERD.output_dir import FileSink
from ..logging import ProgressBar, log

from ..config import Config
from .http_crawler import HttpCrawler, HttpCrawlerSection

@dataclass
class InfomarkTarget:
    course: int
    path: str
    kind: Optional[int]


class InfomarkCrawlerSection(HttpCrawlerSection):
    def target(self) -> InfomarkTarget:
        target = self.s.get("target")
        if not target:
            self.missing_value("target")
        target = target.split("/")

        if len(target) == 2:
            kind = None
        elif len(target) != 3 and target[2].isdigit:
            kind = int(target[2])
        else:
            self.invalid_value("target", target, "Should be <course_id/<sheets | materials | materials/kind_id>")

        if not target[0].isdigit():
            self.invalid_value("target", target, "Should be <course_id/<sheets | materials | materials/kind_id>")
        if not target[1] in ["materials", "sheets"]:
            self.invalid_value("target", target, "Should be <course_id/<sheets | materials | materials/kind_id>")

        return InfomarkTarget(int(target[0]), target[1], kind)

    def base_url(self) -> str:
        base_url = self.s.get("base_url")
        if not base_url:
            self.missing_value("base_url")

        return base_url


@dataclass
class InfomarkFile:
    name: str
    url: str
    id: int
    kind: Optional[int]

    def explain(self) -> None:
        log.explain(f"File {self.name!r} (href={self.url!r})")


class InfomarkCrawler(HttpCrawler):

    def __init__(
            self,
            name: str,
            section: InfomarkCrawlerSection,
            config: Config,
            authenticators: Dict[str, Authenticator]
    ):
        target = section.target()

        super().__init__(name, section, config)
        self._auth = section.auth(authenticators)
        self._base_url = section.base_url()
        self._target_url =  urljoin(section.base_url(), f"api/v1/courses/{target.course}/{target.path}")
        self._filter_kind = target.kind

    async def _fetch_index(self, url) -> List[InfomarkFile]:
        files = []
        async with self.session.get(url) as resp:
            for entry in await resp.json():
                kind = entry.get("kind")
                if self._filter_kind == None or kind == self._filter_kind:
                    id = entry["id"]
                    log.print(f"{entry}")
                    files.append(InfomarkFile(entry["name"], f"{url}/{id}/file", id, kind))

        return files

    async def _crawl_entries(self):
        log.explain("Crawling sheets")
        path = PurePath(".")
        if not await self.crawl(path):
            return

        tasks = []
        for entry in await self._fetch_index(self._target_url):
            etag, mtime = await self._request_resource_version(entry.url)
            tasks.append(self._download_file(path, entry, etag, mtime))

        await self.gather(tasks)

    async def _authenticate(self) -> None:
        username, password = await self._auth.credentials()
        login_data = {
            "email": username,
            "plain_password": password 
        }
        url = urljoin(self._base_url, "api/v1/auth/sessions")
        log.print(f"url: {url}")

        # Start session to handle cookies
        async with self.session.post(url, json=login_data) as resp:
            # Print response
            print("Status Code:", resp.status)
            print("Response Body:", await resp.text())
            if resp.status != 200:
                self._auth.invalidate_credentials()
        #async with self.session.post(url, json=login_data) as request:


    async def _download_file(
        self,
        parent: PurePath,
        file: InfomarkFile,
        etag: Optional[str],
        mtime: Optional[datetime]
    ) -> None:
        element_path = parent / file.name

        prev_etag = self._get_previous_etag_from_report(element_path)
        etag_differs = None if prev_etag is None else prev_etag != etag

        maybe_dl = await self.download(element_path, etag_differs=etag_differs, mtime=mtime)
        if not maybe_dl:
            # keep storing the known file's etag
            if prev_etag:
                self._add_etag_to_report(element_path, prev_etag)
            return

        async with maybe_dl as (bar, sink):
            await self._stream_from_url(file.url, element_path, sink, bar)

    async def _stream_from_url(self, url: str, path: PurePath, sink: FileSink, bar: ProgressBar) -> None:
        async with self.session.get(url, allow_redirects=False) as resp:
            if resp.content_length:
                bar.set_total(resp.content_length)

            async for data in resp.content.iter_chunked(1024):
                sink.file.write(data)
                bar.advance(len(data))

            sink.done()

            self._add_etag_to_report(path, resp.headers.get("ETag"))

    async def _run(self) -> None:
        log.explain("Running crawler")
        auth_id = await self._current_auth_id()
        await self.authenticate(auth_id)
        await self._crawl_entries()

