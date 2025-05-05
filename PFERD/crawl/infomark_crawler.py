from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import List, Optional

from PFERD.output_dir import FileSink
from ..logging import ProgressBar, log

from ..config import Config
from .http_crawler import HttpCrawler, HttpCrawlerSection

class InfomarkCrawlerSection(HttpCrawlerSection):
    def target(self) -> str:
        target = self.s.get("target")
        if not target:
            self.missing_value("target")

        if not target.startswith("https://"):
            self.invalid_value("target", target, "Should be a URL")

        return target


@dataclass
class InfomarkFile:
    name: str
    url: str

    def explain(self) -> None:
        log.explain(f"File {self.name!r} (href={self.url!r})")


class InfomarkCrawler(HttpCrawler):

    def __init__(
            self,
            name: str,
            section: InfomarkCrawlerSection,
            config: Config,
    ):
        super().__init__(name, section, config)
        self._url = section.target()

    async def _fetch_sheets(self) -> List[InfomarkFile]:
        url = self._url + "/api/v1/courses/1/sheets"
        files = []
        async with self.session.get(url) as resp:
            for sheet in await resp.json():
                files.append(InfomarkFile(sheet["name"], f"{url}/{sheet["id"]}/file"))

        return files

    async def _crawl_sheets(self):
        path = PurePath("./sheets")
        if not await self.crawl(path):
            return

        tasks = []
        for entry in await self._fetch_sheets():
            # do this here to at least be sequential and not parallel (rate limiting is hard, as the crawl
            # abstraction does not hold for these requests)
            etag, mtime = await self._request_resource_version(entry.url)
            tasks.append(self._download_file(path, entry, etag, mtime))

        await self.gather(tasks)

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

