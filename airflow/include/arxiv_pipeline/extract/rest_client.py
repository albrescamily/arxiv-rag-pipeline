import requests
import time
import feedparser

#create the get endpoint

base_url = "https://export.arxiv.org/api/query"


def parse_entry(entry) -> dict:

    paper = {
        "id": entry.id.split("/abs/")[-1],
        "title": entry.title.strip(),
        "summary": entry.summary.strip(),
        "published": entry.published,
        "updated": entry.updated,
        "authors": [
            author.name
            for author in entry.authors
        ],

        "categories": [
            tag["term"]
            for tag in entry.tags
        ]
    }

    return paper


def fetch_page(
    search_query: str,
    start: int,
    batch_size: int,
    session=None
):
    params = {
        "search_query": search_query,
        "start": start,
        "max_results": batch_size,
        "sortBy": "submittedDate",
        "sortOrder": "ascending"
    }

    # reuse connection between requests
    requester = session or requests

    response = requester.get(
        base_url,
        params=params,
        timeout=(5, 30)
    )

    response.raise_for_status()

    feed = feedparser.parse(response.content)

    # malformed XML/feed
    if feed.bozo:
        raise ValueError(
            f"Malformed feed: {feed.bozo_exception}"
        )

    # arxiv can return HTTP 200 with an API error inside the feed
    if (
        len(feed.entries) == 1
        and "api/errors" in feed.entries[0].id
    ):
        raise ValueError(
            f"arXiv rejected the query: "
            f"{feed.entries[0].summary}"
        )

    return feed


def _iter_pages(
    search_query: str,
    batch_size: int,
    wait_time: int = 3
):
    start = 0
    total = None

    with requests.Session() as session:

        while True:

            began = time.monotonic()

            # avoid requesting more entries than remain
            current_batch_size = batch_size

            if total is not None:
                current_batch_size = min(
                    batch_size,
                    total - start
                )

            feed = fetch_page(
                search_query=search_query,
                start=start,
                batch_size=current_batch_size,
                session=session
            )

            # first request tells us how many results exist
            if total is None:

                total = int(
                    feed.feed.opensearch_totalresults
                )
                print(total)

                if total == 0:
                    return

            # do not silently finish an incomplete extraction
            if not feed.entries:
                raise RuntimeError(
                    f"Empty page returned by arXiv "
                    f"at start={start}/{total}"
                )

            papers = []

            for entry in feed.entries:

                paper = parse_entry(entry)

                papers.append(paper)

            # return one batch at a time
            yield papers

            # advance by the number actually received
            start += len(feed.entries)

            if start >= total:
                break

            # respect interval between requests
            elapsed = time.monotonic() - began

            time.sleep(
                max(0, wait_time - elapsed)
            )


def fetch_raw_data(
    search_query: str,
    batch_size: int,
    wait_time: int = 3
):
    # flatten the per-page batches into one list -- Airflow's TaskFlow
    # return value is pushed to XCom as JSON, and a live generator
    # can't be serialized (and callers shouldn't have to know about paging)
    papers = []

    for batch in _iter_pages(
        search_query=search_query,
        batch_size=batch_size,
        wait_time=wait_time
    ):
        papers.extend(batch)

    return papers


# if __name__ == "__main__":

#     search_query = (
#         "cat:cs.AI OR "
#         "cat:cs.LG OR "
#         "cat:cs.CL"
#     )

#     total = 0

#     for batch in fetch_raw_data(
#         search_query=search_query,
#         batch_size=5
#     ):

#         total += len(batch)

#         print(
#             f"Received {len(batch)} papers "
#             f"- total: {total}"
#         )

#         for paper in batch:
#             print(paper["title"])