from airflow.sdk import dag, task, Variable
from datetime import datetime

from arxiv_pipeline.extract.rest_client import fetch_raw_data
from arxiv_pipeline.transform.transformations import matches_topics, TOPIC_KEYWORDS


@dag(
    dag_id="arxiv_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)
def arxiv_pipeline():

    @task
    def extract(data_interval_start=None, data_interval_end=None):
        # airflow injects these two from the run's own interval — same query, run
        # today or backfilled a year from now, always scoped to that one day
        window = (
            f"submittedDate:[{data_interval_start.strftime('%Y%m%d%H%M')}"
            f" TO {data_interval_end.strftime('%Y%m%d%H%M')}]"
        )

        return fetch_raw_data(
            search_query=(
            "(cat:cs.AI OR "
            "cat:cs.LG OR "
            "cat:cs.CL) "
            f"AND {window}"
            ),
            batch_size=20,
        )
        
    @task
    def transform(papers):
        topics = Variable.get(
            "arxiv_topics",
            default=TOPIC_KEYWORDS,
            deserialize_json=True,
        )

        filtered_papers = []

        for paper in papers:

            if matches_topics(
                paper=paper,
                keywords=topics,
            ):
                filtered_papers.append(paper)

        return filtered_papers

    @task
    def load(papers):
        # save on PostgreSQL
        print(f"Saving {len(papers)} papers")

    raw = extract()
    transformed = transform(raw)
    load(transformed)


arxiv_pipeline()