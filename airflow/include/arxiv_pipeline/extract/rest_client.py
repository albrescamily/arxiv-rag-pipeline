import requests
import time
import feedparser

#create the get endpoint

base_url = "https://export.arxiv.org/api/query"



def fetch_page(search_query:str, start:int, batch_size:int, session=None):
    params = {
        "search_query": search_query,
        "start": start,
        "max_results": batch_size,
        "sortBy": "submittedDate",
        "sortOrder": "ascending" #top of the last doesnt move
    }

    # reuse the caller's connection when there is one, so paging skips the tls handshake
    requester = session or requests

    response = requester.get(base_url, params=params, timeout=(5, 30))
    response.raise_for_status()

    feed = feedparser.parse(response.content)

    if feed.bozo:
        raise ValueError(f"malformed feed: {feed.bozo_exception}")

    # arxiv answers 200 with an error feed instead of a failing status code
    if len(feed.entries) == 1 and "api/errors" in feed.entries[0].id:
        raise ValueError(f"arxiv rejected the query: {feed.entries[0].summary}")

    return feed


def fetch_raw_data(search_query:str, max_results:int|None=None, batch_size:int=2000, wait_time:int=3):

    start = 0
    limit = None
    papers = []

    # never ask for more per call than the caller wants in total
    if max_results is not None:
        batch_size = min(batch_size, max_results)

    with requests.Session() as session:

        while True:

            began = time.monotonic()

            feed = fetch_page(search_query, start, batch_size, session=session)

            # first pass only: arxiv tells us how many results the query really has
            if limit is None:
                total = int(feed.feed.opensearch_totalresults)
                if total == 0:
                    return []
                limit = total if max_results is None else min(max_results, total)

            # arxiv sometimes returns an empty page while more results exist
            if not feed.entries:
                break

            for entry in feed.entries:
                paper = {
                    "id": entry.id.split("/abs/")[-1],
                    "title": entry.title,
                    "summary": entry.summary,
                    "published": entry.published,
                    "updated": entry.updated,
                    "authors": [
                        author.name
                        for author in entry.authors
                    ],
                }

                papers.append(paper)

            # advance by what actually came back, not by what we asked for
            start += len(feed.entries)

            if start >= limit:
                break

            # last page only needs the remainder
            batch_size = min(batch_size, limit - start)

            # the 3s arxiv asks for counts from the start of the request, not from now
            elapsed = time.monotonic() - began
            time.sleep(max(0, wait_time - elapsed))

    return papers[:limit]


if __name__ == "__main__":
    papers = fetch_raw_data(
        search_query="all:biophysics",
        max_results=10,
    )

    print(f"{len(papers)} papers")
    for paper in papers:
        print(paper["title"])
