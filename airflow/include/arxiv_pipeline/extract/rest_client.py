import requests
import time
import feedparser

#create the get endpoint 

base_url = "https://export.arxiv.org/api/query"


def fetch_raw_data(search_query:str,max_results:int,batch_size:int,wait_time:int=3):

    papers = []

    for start in range(0,max_results,batch_size):
        params = {
            "search_query":search_query,
            "start": start,
            "max_results":batch_size,
            "sortBy": "submittedDate",
            "sortOrder": "descending"
            }


        resp = requests.get(base_url, params=params)
        resp.raise_for_status()

        feed = feedparser.parse(resp.content)

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

        time.sleep(wait_time)

    return papers


if __name__ == "__main__":
    papers = fetch_raw_data(
        search_query="all:biophysics",
        max_results=10,
        batch_size=5,
    )

    for paper in papers:
        print(paper["title"])

