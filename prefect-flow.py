"""DS 3022 Data Project 2: Prefect flow.

Pipeline (one run, no human steps):
  populate_queue -> prepare_landing -> collect_messages -> dbt_build -> read_phrase -> submit_solution

Run:   python prefect-flow.py
Then:  python kit/check_submission.py
"""
import os
import time

import boto3
import duckdb
import requests
from dbt.cli.main import dbtRunner
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# .env has to load BEFORE boto3.client() so it picks up the emulator endpoint and test keys
load_dotenv()

UVA_ID = os.environ["UVA_ID"]                        # never hard-code my ID, it comes from .env
PORT = os.environ.get("SCATTER_PORT", "8000")
SCATTER_URL = f"http://127.0.0.1:{PORT}/api/scatter/{UVA_ID}"   # 127.0.0.1, not localhost (slow on Windows)
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")         # same file dbt reads (see dbt/profiles.yml)

sqs = boto3.client("sqs")  # endpoint comes from AWS_ENDPOINT_URL_SQS, so no URL or keys in the code


# ---------------------------------------------------------------- Checkpoint A

@task(retries=2, retry_delay_seconds=5)
def populate_queue() -> str:
    """POST to the scatter API once and return my queue URL."""
    logger = get_run_logger()
    # retries only cover the POST itself failing - this task never runs again once collecting starts,
    # because every POST purges the queue and starts a brand new puzzle
    resp = requests.post(SCATTER_URL, timeout=30)
    resp.raise_for_status()                       # a 4xx/5xx should fail loudly, not carry on
    payload = resp.json()
    logger.info("Scatter API answered: %s", payload)
    return payload["sqs_url"]                     # this return value is the arrow to the next task in the DAG


COUNTERS = ["ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"]


def get_counts(queue_url: str) -> dict:
    """Plain helper (not a task): the three queue counters as ints."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=COUNTERS)["Attributes"]
    # SQS hands these back as strings, so cast them or sum() later won't work
    return {"visible":   int(attrs["ApproximateNumberOfMessages"]),
            "in_flight": int(attrs["ApproximateNumberOfMessagesNotVisible"]),
            "delayed":   int(attrs["ApproximateNumberOfMessagesDelayed"])}


@task
def monitor_queue(queue_url: str, poll_s: int = 5,
                  timeout_s: int = 1200) -> dict:
    """Log the counters until nothing is delayed; give up after timeout_s.
    (Checkpoint A work - the final flow doesn't call it, collect_messages watches the counters itself.)"""
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        c = get_counts(queue_url)
        logger.info("visible=%d  in_flight=%d  delayed=%d",
                    c["visible"], c["in_flight"], c["delayed"])
        if c["delayed"] == 0:
            logger.info("All messages released")
            return c
        time.sleep(poll_s)
    # a clear failure beats a flow that hangs forever
    raise TimeoutError(f"Messages still delayed after {timeout_s} s")


# ---------------------------------------------------------------- Checkpoint B (+ Lesson 11 hardening)

@task
def prepare_landing() -> None:
    """Fresh, empty landing tables for this run."""
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("create schema if not exists raw")
        # order_no stays varchar here, exactly as it arrives - dbt casts it to an int later
        con.execute("create or replace table raw.fragments ("
                    "order_no varchar, word varchar, "
                    "received_at timestamp)")
        # chaos round: broken messages get parked here so I can inspect them afterwards
        con.execute("create or replace table raw.rejects ("
                    "message_id varchar, attributes varchar, "
                    "received_at timestamp)")
    # separate task on purpose: if collect_messages is retried it must NOT wipe rows
    # whose messages are already deleted from the queue


def stored_count(con) -> int:
    """How many DIFFERENT fragments are stored so far."""
    # distinct, so a message delivered twice adds a row but never counts twice
    return con.execute("select count(distinct order_no) "
                       "from raw.fragments").fetchone()[0]


def parse(msg) -> tuple[str, str] | None:
    """Return (order_no, word), or None if the message is malformed (poison)."""
    a = msg.get("MessageAttributes", {})
    try:
        order_no = a["order_no"]["StringValue"]
        word = a["word"]["StringValue"]
    except KeyError:
        return None                       # an attribute is missing (the chaos poison has no 'word')
    if not order_no.isdigit():
        return None                       # position isn't a number, e.g. '?' - dbt could never cast it
    return order_no, word


@task
def collect_messages(queue_url: str, expected: int = 21,
                     timeout_s: int = 1200) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s     # 1200 s > the 900 s max delay, so full-delay runs fit
    with duckdb.connect(DUCKDB_PATH) as con:    # one connection for the whole loop
        while time.monotonic() < deadline:
            stored, c = stored_count(con), get_counts(queue_url)
            # this line every pass is my polling evidence in the logs
            logger.info("stored %d/%d | visible=%d in_flight=%d delayed=%d",
                        stored, expected, c["visible"],
                        c["in_flight"], c["delayed"])
            # stop rule: 21 different fragments AND nothing left anywhere in the queue
            if stored >= expected and sum(c.values()) == 0:
                return stored
            resp = sqs.receive_message(QueueUrl=queue_url,
                                       MaxNumberOfMessages=10,
                                       MessageAttributeNames=["All"],  # without this the attributes don't come back
                                       WaitTimeSeconds=10)             # long poll: wait up to 10 s if nothing's visible
            # an empty reply has no "Messages" key at all, .get stops that being a KeyError
            for msg in resp.get("Messages", []):
                fragment = parse(msg)
                if fragment is None:                                   # poison message
                    logger.warning("Rejected malformed message %s",
                                   msg["MessageId"])
                    con.execute("insert into raw.rejects "
                                "values (?, ?, current_timestamp)",
                                [msg["MessageId"],
                                 str(msg.get("MessageAttributes"))])
                else:
                    # save FIRST - if we crash after this, the worst case is a duplicate, never a lost word
                    con.execute("insert into raw.fragments "
                                "values (?, ?, current_timestamp)",
                                list(fragment))
                # delete sits outside the if/else on purpose: good or bad, every message gets deleted,
                # otherwise the poison comes back every 30 s and the queue never reaches 0
                sqs.delete_message(QueueUrl=queue_url,
                                   ReceiptHandle=msg["ReceiptHandle"])   # ReceiptHandle, not MessageId
        stored = stored_count(con)
    raise TimeoutError(f"Only {stored}/{expected} fragments "
                       f"after {timeout_s} s")


# ---------------------------------------------------------------- Checkpoint C

@task  # no retries: a failed test is a data problem, not a glitch
def dbt_build() -> None:
    """Run dbt build; raise if anything fails (this is the quality gate)."""
    res = dbtRunner().invoke(["build", "--project-dir", "dbt",
                              "--profiles-dir", "dbt"])
    if not res.success:
        # raising here means read_phrase and submit_solution never run on bad data
        raise RuntimeError("dbt tests failed: not submitting")
    get_run_logger().info("dbt build passed: all tests green")


@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    # no read_only=True: dbt still has its own connection open in this same process
    with duckdb.connect(DUCKDB_PATH) as con:
        n, phrase = con.execute("select fragment_count, phrase "
                                "from assembled_phrase").fetchone()
    get_run_logger().info("Assembled %d fragments: %s", n, phrase)
    return phrase


# ---------------------------------------------------------------- Checkpoint D

@task(retries=2, retry_delay_seconds=5)   # sending is a network call, so a retry is fine here
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes."""
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]   # the scatter API already created this queue
    resp = sqs.send_message(
        QueueUrl=url, MessageBody="dp2 solution",                 # body doesn't matter, the checker reads attributes
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": UVA_ID},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform}})
    status = resp["ResponseMetadata"]["HTTPStatusCode"]
    get_run_logger().info("Submitted, HTTP %s", status)
    if status != 200:
        raise RuntimeError(f"Submission returned HTTP {status}")
    return status


# ---------------------------------------------------------------- the flow

@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline(timeout_s: int = 1200):
    # six tasks, one line each - every task raises on failure so nothing after it runs
    queue_url = populate_queue()
    prepare_landing()
    collect_messages(queue_url, timeout_s=timeout_s)
    dbt_build()
    phrase = read_phrase()
    submit_solution(phrase)


if __name__ == "__main__":
    dp2_pipeline()
