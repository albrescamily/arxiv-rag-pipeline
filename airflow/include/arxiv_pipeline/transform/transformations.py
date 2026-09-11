# transform/transformations.py

TOPIC_KEYWORDS = [
    "machine learning",
    "gpu",
    "agent",
    "llm",
    "large language model",
]


def matches_topics(paper: dict, keywords: list[str] = TOPIC_KEYWORDS) -> bool:

    text = (
        paper["title"]
        + " "
        + paper["summary"]
    ).lower()

    return any(
        keyword in text
        for keyword in keywords
    )