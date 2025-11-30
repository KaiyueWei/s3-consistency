import os, time, json, uuid, datetime
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import numpy as np
import pandas as pd
import uuid, time

# ---------- Config ----------
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
SESSION = boto3.Session(region_name=AWS_REGION, profile_name=os.environ.get("AWS_PROFILE"))
S3 = SESSION.client("s3", config=Config(retries={"max_attempts": 10, "mode": "standard"}))

TEST_BUCKET = f"cs6620-s3-consistency-{datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
DATA_PLANE_OBJECTS = 100
CONTROL_PLANE_ITERS = 100

# Polling profile for control-plane verification
EARLY_DENSE_SECONDS = 30        # 1s polling for first 30s
LATE_INTERVAL_SECONDS = 3       # after that, poll every 3s
TIMEOUT_SECONDS = 600           # hard cap for measurement

# Two alternating lifecycle configs (A/B)
def lifecycle_config(days:int, rule_id:str):
    return {
        "Rules": [{
            "ID": rule_id,
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "Expiration": {"Days": days}
        }]
    }

CONFIG_A = lifecycle_config(30, "expire-old-files-A")
CONFIG_B = lifecycle_config(31, "expire-old-files-B")

# ---------- Helpers ----------
def create_bucket(bucket, region):
    params = {"Bucket": bucket}
    if region != "us-east-1":
        params["CreateBucketConfiguration"] = {"LocationConstraint": region}
    S3.create_bucket(**params)
    waiter = S3.get_waiter("bucket_exists")
    waiter.wait(Bucket=bucket)

def delete_bucket_and_contents(bucket):
    try:
        # Delete objects
        resp = S3.list_objects_v2(Bucket=bucket)
        while True:
            if "Contents" in resp:
                to_delete = [{"Key": obj["Key"]} for obj in resp["Contents"]]
                if to_delete:
                    S3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
            if resp.get("IsTruncated"):
                resp = S3.list_objects_v2(Bucket=bucket, ContinuationToken=resp["NextContinuationToken"])
            else:
                break
        # Remove lifecycle (just in case)
        try:
            S3.delete_bucket_lifecycle(Bucket=bucket)
        except ClientError:
            pass
        # Delete bucket
        S3.delete_bucket(Bucket=bucket)
    except ClientError as e:
        print("Cleanup warning:", e)

def equal_lifecycle(a, b):
    # Normalize dicts for comparison (sort keys)
    return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)

# ---------- Test 1.1: Data Plane ----------
def test_data_plane(bucket):
    rows = []
    for i in range(DATA_PLANE_OBJECTS):
        key = f"obj-{i:03d}-{uuid.uuid4().hex[:8]}.txt"
        body = f"hello-{i}-{uuid.uuid4()}".encode()

        t0 = time.perf_counter()
        S3.put_object(Bucket=bucket, Key=key, Body=body)
        # immediate read-after-write
        try:
            obj = S3.get_object(Bucket=bucket, Key=key)
            _ = obj["Body"].read()
        except ClientError as e:
            # Should not happen under strong consistency
            rows.append({"key": key, "success": False, "latency_ms": None, "error": str(e)})
            continue
        t1 = time.perf_counter()
        rows.append({"key": key, "success": True, "latency_ms": round((t1 - t0)*1000, 2)})

    df = pd.DataFrame(rows)
    df.to_csv("data_plane_results.csv", index=False)

    succ = df[df["success"]]
    stats = {}
    if not succ.empty:
        lat = succ["latency_ms"].astype(float).to_numpy()
        stats = {
            "count": int(len(lat)),
            "success_rate": round(100*len(succ)/len(df), 2),
            "mean_ms": round(np.mean(lat), 2),
            "median_ms": round(np.median(lat), 2),
            "p95_ms": round(np.percentile(lat, 95), 2),
            "p99_ms": round(np.percentile(lat, 99), 2),
            "max_ms": round(np.max(lat), 2),
        }
    return stats

# ---------- Test 1.2: Control Plane ----------
def put_lifecycle(bucket, config):
    S3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=config)

def get_lifecycle(bucket):
    # Returns normalized dict form of current lifecycle config
    resp = S3.get_bucket_lifecycle_configuration(Bucket=bucket)
    return {"Rules": resp["Rules"]}

def poll_until_match(bucket, expected_config, timeout_s=TIMEOUT_SECONDS):
    start = time.perf_counter()
    polls = 0
    first_snapshot = None
    first_snapshot_age = None

    # 1s polling for EARLY_DENSE_SECONDS, then every LATE_INTERVAL_SECONDS
    while True:
        elapsed = time.perf_counter() - start
        if elapsed > timeout_s:
            return {
                "matched": False,
                "elapsed": elapsed,
                "polls": polls,
                "first_read": first_snapshot,
                "first_read_elapsed": first_snapshot_age,
            }

        try:
            current = get_lifecycle(bucket)
            polls += 1
            observed_age = time.perf_counter() - start

            if first_snapshot is None:
                first_snapshot = current
                first_snapshot_age = observed_age

            if equal_lifecycle(current, expected_config):
                return {
                    "matched": True,
                    "elapsed": observed_age,
                    "polls": polls,
                    "first_read": first_snapshot,
                    "first_read_elapsed": first_snapshot_age,
                }
        except ClientError as e:
            # Right after PUT, GET may briefly 404 if not yet visible; tolerate
            err = e.response["Error"]["Code"]
            if err not in ("NoSuchLifecycleConfiguration", "NoSuchBucket", "AccessDenied"):
                # Unexpected—surface it
                raise

        # sleep cadence
        current_age = time.perf_counter() - start
        if current_age < EARLY_DENSE_SECONDS:
            time.sleep(1)
        else:
            time.sleep(LATE_INTERVAL_SECONDS)

def test_control_plane(bucket):
    rows = []
    prev_cfg = None
    for i in range(CONTROL_PLANE_ITERS):
        rule_id = f"expire-{uuid.uuid4()}"
        cfg = {
            "Rules": [{
                "ID": rule_id,
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "Expiration": {"Days": 30 + (i % 7)}
            }]
        }
        put_lifecycle(bucket, cfg)
        result = poll_until_match(bucket, cfg, TIMEOUT_SECONDS)
        first_read = result["first_read"]
        first_read_rule = None
        if first_read and first_read.get("Rules"):
            first_read_rule = first_read["Rules"][0].get("ID")

        first_matches = bool(first_read and equal_lifecycle(first_read, cfg))
        first_is_prev = bool(first_read and prev_cfg and equal_lifecycle(first_read, prev_cfg))

        rows.append({
            "iteration": i,
            "matched": result["matched"],
            "propagation_sec": round(result["elapsed"], 3),
            "polls": result["polls"],
            "timeout_used": TIMEOUT_SECONDS,
            "first_read_sec": round(result["first_read_elapsed"], 3) if result["first_read_elapsed"] is not None else None,
            "first_read_rule": first_read_rule,
            "first_read_matched": first_matches,
            "first_read_stale": first_is_prev,
        })
        prev_cfg = cfg
        time.sleep(10)  # <-- important: allow real propagation

    df = pd.DataFrame(rows)
    df.to_csv("control_plane_results.csv", index=False)

    # Stats including virtual timeouts at 180s and 300s
    times = df["propagation_sec"].to_numpy()
    matched = df["matched"].to_numpy()
    succ_times = times[matched]

    def pct(x): return round(float(x), 2)

    first_match_rate = df["first_read_matched"].fillna(False).astype(float).mean()
    stale_rate = df["first_read_stale"].fillna(False).astype(float).mean()

    stats = {
        "n": len(df),
        "success_rate_600s_%": pct(100*np.mean(matched)),
        "success_rate_180s_%": pct(100*np.mean(times <= 180.0)),
        "success_rate_300s_%": pct(100*np.mean(times <= 300.0)),
        "instant_visibility_%": pct(100*first_match_rate),
        "stale_first_read_%": pct(100*stale_rate),
    }
    if len(succ_times) > 0:
        stats.update({
            "mean_s": pct(np.mean(succ_times)),
            "median_s": pct(np.median(succ_times)),
            "p95_s": pct(np.percentile(succ_times, 95)),
            "p99_s": pct(np.percentile(succ_times, 99)),
            "max_s": pct(np.max(succ_times)),
        })
    return stats

# ---------- Main ----------
if __name__ == "__main__":
    print(f"Creating bucket: {TEST_BUCKET} in {AWS_REGION}")
    create_bucket(TEST_BUCKET, AWS_REGION)

    try:
        print("\n[TEST 1.1] Data Plane Strong Consistency")
        dp_stats = test_data_plane(TEST_BUCKET)
        print("Results:", dp_stats)

        print("\n[TEST 1.2] Control Plane Propagation (Lifecycle)")
        cp_stats = test_control_plane(TEST_BUCKET)
        print("Results:", cp_stats)

        print("\nCSV written: data_plane_results.csv, control_plane_results.csv")
    finally:
        print("\nCleaning up …")
        delete_bucket_and_contents(TEST_BUCKET)
        print("Done.")
