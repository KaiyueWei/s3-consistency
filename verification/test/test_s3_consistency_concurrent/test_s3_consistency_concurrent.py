import os, time, json, uuid, datetime, random
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Config ----------
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
SESSION = boto3.Session(region_name=AWS_REGION, profile_name=os.environ.get("AWS_PROFILE"))
S3 = SESSION.client("s3", config=Config(retries={"max_attempts": 10, "mode": "standard"}))

TEST_BUCKET = f"cs6620-s3-consistency-concurrent-{datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
DATA_PLANE_OBJECTS = 100
CONTROL_PLANE_ITERS = 100

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

def lifecycle_config(days:int, rule_id:str):
    return {
        "Rules": [{
            "ID": rule_id,
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "Expiration": {"Days": days}
        }]
    }

# ---------- Test 2.1: Data Plane Concurrent ----------
def verify_atomic_write(bucket, index):
    key = f"concurrent-obj-{index}-{uuid.uuid4().hex[:6]}"
    body = b"x" * 1024
    
    # 1. Write
    t0 = time.perf_counter()
    try:
        S3.put_object(Bucket=bucket, Key=key, Body=body)
        t_write_ack = time.perf_counter()
        
        # 2. Immediate Read (Strong Consistency Check)
        S3.get_object(Bucket=bucket, Key=key)
        t_read_ack = time.perf_counter()
        
        return {
            "key": key,
            "success": True, 
            "write_latency_ms": (t_write_ack - t0) * 1000,
            "read_latency_ms": (t_read_ack - t_write_ack) * 1000,
            "total_latency_ms": (t_read_ack - t0) * 1000
        }
    except ClientError as e:
        return {
            "key": key,
            "success": False, 
            "error": str(e)
        }

def test_data_plane_concurrent(bucket):
    print(f"   Launch: {DATA_PLANE_OBJECTS} parallel threads...")
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(verify_atomic_write, bucket, i) for i in range(DATA_PLANE_OBJECTS)]
        for f in as_completed(futures):
            results.append(f.result())
    
    df = pd.DataFrame(results)
    df.to_csv("data_plane_concurrent_results.csv", index=False)

    succ = df[df["success"] == True]
    stats = {}
    if not succ.empty:
        lat = succ["total_latency_ms"].astype(float).to_numpy()
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

# ---------- Test 2.2: Control Plane Racer ----------
def smart_poll_until_match(bucket, expected_rule_id, start_time, timeout=600):
    """
    Uses Exponential Backoff + Jitter to minimize API calls (cost) 
    while maintaining precision.
    """
    attempt = 0
    base_sleep = 0.5
    max_sleep = 10.0
    
    while (time.perf_counter() - start_time) < timeout:
        attempt += 1
        try:
            # Get current config
            resp = S3.get_bucket_lifecycle_configuration(Bucket=bucket)
            rules = resp.get("Rules", [])
            
            # Check if our NEW rule ID is present
            if any(r["ID"] == expected_rule_id for r in rules):
                return {
                    "found": True, 
                    "attempts": attempt, 
                    "detected_at": time.perf_counter()
                }
        except ClientError:
            pass # Lifecycle might not exist yet
        
        # Exponential Backoff with "Full Jitter"
        # Sleep = random_between(0, min(cap, base * 2^attempt))
        sleep_time = random.uniform(0, min(max_sleep, base_sleep * (2 ** attempt)))
        time.sleep(sleep_time)

    return {"found": False, "attempts": attempt, "detected_at": None}

def test_control_plane_racer(bucket):
    print(f"   Running Control Plane Racer ({CONTROL_PLANE_ITERS} iterations)...")
    stats = []
    
    for i in range(CONTROL_PLANE_ITERS):
        rule_id = f"rule-{uuid.uuid4().hex}"
        config = lifecycle_config(30, rule_id)
        
        # Mark time 0
        t0 = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            # THREAD A: The Poller (Starts IMMEDIATELY)
            # We pass t0 so it knows when the "race" began
            poller_future = executor.submit(smart_poll_until_match, bucket, rule_id, t0)
            
            # THREAD B: The Writer
            # We introduce a tiny sleep to ensure Poller is alive, then Write
            time.sleep(0.1) 
            S3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=config)
            
            # Wait for result
            result = poller_future.result()
        
        if result["found"]:
            propagation_delay = result["detected_at"] - t0
        else:
            propagation_delay = None

        stats.append({
            "iteration": i, 
            "found": result["found"],
            "delay_sec": propagation_delay, 
            "api_calls": result["attempts"]
        })
        
        # Don't sleep 10s. Just randomize rule IDs (which you do) 
        # and proceed. This stresses the control plane harder.
        
    df = pd.DataFrame(stats)
    df.to_csv("control_plane_racer_results.csv", index=False)

    # Stats
    found_df = df[df["found"] == True]
    delays = found_df["delay_sec"].astype(float).to_numpy()
    
    def pct(x): return round(float(x), 2)

    stats_summary = {
        "n": len(df),
        "success_rate_%": pct(100*len(found_df)/len(df)),
    }
    
    if len(delays) > 0:
        stats_summary.update({
            "mean_s": pct(np.mean(delays)),
            "median_s": pct(np.median(delays)),
            "p95_s": pct(np.percentile(delays, 95)),
            "p99_s": pct(np.percentile(delays, 99)),
            "max_s": pct(np.max(delays)),
        })
        
    return stats_summary

# ---------- Main ----------
if __name__ == "__main__":
    print(f"Creating bucket: {TEST_BUCKET} in {AWS_REGION}")
    create_bucket(TEST_BUCKET, AWS_REGION)

    try:
        print("\n[TEST 2.1] Data Plane Concurrent (Thundering Herd)")
        dp_stats = test_data_plane_concurrent(TEST_BUCKET)
        print("Results:", dp_stats)

        print("\n[TEST 2.2] Control Plane Racer")
        cp_stats = test_control_plane_racer(TEST_BUCKET)
        print("Results:", cp_stats)

        print("\nCSV written: data_plane_concurrent_results.csv, control_plane_racer_results.csv")
    finally:
        print("\nCleaning up …")
        delete_bucket_and_contents(TEST_BUCKET)
        print("Done.")
