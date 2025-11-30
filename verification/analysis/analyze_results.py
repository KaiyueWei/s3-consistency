import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

# Define paths relative to this script
BASE_TEST_DIR = "../test"
SEQ_DIR = os.path.join(BASE_TEST_DIR, "test_s3_consistency")
CONC_DIR = os.path.join(BASE_TEST_DIR, "test_s3_consistency_concurrent")

# File paths
DATA_PLANE_SEQ = os.path.join(SEQ_DIR, "data_plane_results.csv")
CONTROL_PLANE_SEQ = os.path.join(SEQ_DIR, "control_plane_results.csv")
DATA_PLANE_CONC = os.path.join(CONC_DIR, "data_plane_concurrent_results.csv")
CONTROL_PLANE_CONC = os.path.join(CONC_DIR, "control_plane_racer_results.csv")

OUTPUT_REPORT = "analysis_report.txt"

def load_csv(path):
    if not os.path.exists(path):
        print(f"Warning: File not found: {path}")
        return None
    return pd.read_csv(path)

def log(message, file_handle=None):
    print(message)
    if file_handle:
        file_handle.write(message + "\n")

def analyze_data_plane(df_seq, df_conc, f):
    log("\n" + "="*60, f)
    log("DATA PLANE ANALYSIS (Object Consistency)", f)
    log("="*60, f)

    # Sequential Analysis
    if df_seq is not None:
        log(f"\n[Sequential Test] (N={len(df_seq)})", f)
        success_seq = df_seq[df_seq['success'] == True]
        log(f"  Success Rate: {len(success_seq)/len(df_seq)*100:.2f}%", f)
        if not success_seq.empty:
            lat = success_seq['latency_ms']
            log(f"  Latency (ms): Mean={lat.mean():.2f}, Median={lat.median():.2f}, P99={lat.quantile(0.99):.2f}", f)

    # Concurrent Analysis
    if df_conc is not None:
        log(f"\n[Concurrent Test] (N={len(df_conc)})", f)
        success_conc = df_conc[df_conc['success'] == True]
        log(f"  Success Rate: {len(success_conc)/len(df_conc)*100:.2f}%", f)
        if not success_conc.empty:
            # Concurrent has breakdown
            log("  Latency Breakdown (ms):", f)
            log(f"    Write: Mean={success_conc['write_latency_ms'].mean():.2f}, P99={success_conc['write_latency_ms'].quantile(0.99):.2f}", f)
            log(f"    Read:  Mean={success_conc['read_latency_ms'].mean():.2f}, P99={success_conc['read_latency_ms'].quantile(0.99):.2f}", f)
            log(f"    Total: Mean={success_conc['total_latency_ms'].mean():.2f}, P99={success_conc['total_latency_ms'].quantile(0.99):.2f}", f)

def analyze_control_plane(df_seq, df_conc, f):
    log("\n" + "="*60, f)
    log("CONTROL PLANE ANALYSIS (Eventual Consistency)", f)
    log("="*60, f)

    # Sequential Analysis
    if df_seq is not None:
        log(f"\n[Sequential Test] (N={len(df_seq)})", f)
        # In sequential test, 'propagation_sec' is the metric
        # matched indicates if it eventually succeeded
        success_seq = df_seq[df_seq['matched'] == True]
        log(f"  Success Rate: {len(success_seq)/len(df_seq)*100:.2f}%", f)
        if not success_seq.empty:
            prop = success_seq['propagation_sec']
            log(f"  Propagation (s): Mean={prop.mean():.2f}, Median={prop.median():.2f}, Max={prop.max():.2f}", f)
            log(f"  Instant Visibility (<1s): {(prop < 1.0).mean()*100:.1f}%", f)

    # Concurrent Analysis
    if df_conc is not None:
        log(f"\n[Concurrent 'Racer' Test] (N={len(df_conc)})", f)
        # In concurrent test, 'found' is success, 'delay_sec' is propagation
        success_conc = df_conc[df_conc['found'] == True]
        log(f"  Success Rate: {len(success_conc)/len(df_conc)*100:.2f}%", f)
        if not success_conc.empty:
            prop = success_conc['delay_sec']
            log(f"  Propagation (s): Mean={prop.mean():.2f}, Median={prop.median():.2f}, Max={prop.max():.2f}", f)
            log(f"  Instant Visibility (<1s): {(prop < 1.0).mean()*100:.1f}%", f)
            
            # Tail latency analysis
            slow = prop[prop > 5.0]
            if not slow.empty:
                log(f"  Tail Latency Events (>5s): {len(slow)} events", f)
                log(f"  Worst Case: {prop.max():.2f}s", f)
            else:
                log("  No tail latency events > 5s detected.", f)

            # API Calls Analysis
            if 'api_calls' in success_conc.columns:
                calls = success_conc['api_calls']
                log(f"  API Calls: Mean={calls.mean():.2f}, Max={calls.max()}, Total={calls.sum()}", f)

def plot_results(df_dp_seq, df_dp_conc, df_cp_seq, df_cp_conc):
    # 1. Data Plane Latency Comparison
    plt.figure(figsize=(10, 6))
    if df_dp_seq is not None:
        plt.hist(df_dp_seq[df_dp_seq['success']]['latency_ms'], bins=30, alpha=0.5, label='Sequential', density=True)
    if df_dp_conc is not None:
        plt.hist(df_dp_conc[df_dp_conc['success']]['total_latency_ms'], bins=30, alpha=0.5, label='Concurrent', density=True)
    
    plt.title('Data Plane Latency Distribution (Sequential vs Concurrent)')
    plt.xlabel('Latency (ms)')
    plt.ylabel('Density')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('data_plane_latency.png')
    plt.close()

    # 2. Control Plane Propagation Delay Comparison
    plt.figure(figsize=(10, 6))
    if df_cp_seq is not None:
        # Filter out timeouts if any, though matched=True handles it
        data = df_cp_seq[df_cp_seq['matched']]['propagation_sec']
        plt.hist(data, bins=30, alpha=0.5, label='Sequential', density=True)
    
    if df_cp_conc is not None:
        data = df_cp_conc[df_cp_conc['found']]['delay_sec']
        plt.hist(data, bins=30, alpha=0.5, label='Concurrent (Racer)', density=True)

    plt.title('Control Plane Propagation Delay (Sequential vs Concurrent)')
    plt.xlabel('Propagation Time (s)')
    plt.ylabel('Density')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('control_plane_propagation.png')
    plt.close()

    # 3. Control Plane Tail Latency (Concurrent Only) - CDF
    if df_cp_conc is not None:
        plt.figure(figsize=(10, 6))
        data = np.sort(df_cp_conc[df_cp_conc['found']]['delay_sec'])
        y = np.arange(1, len(data)+1) / len(data)
        plt.plot(data, y, marker='.', linestyle='none')
        plt.title('CDF of Control Plane Propagation (Concurrent Racer)')
        plt.xlabel('Propagation Time (s)')
        plt.ylabel('Cumulative Probability')
        plt.grid(True, alpha=0.3)
        plt.axvline(x=5.0, color='r', linestyle='--', label='5s Threshold')
        plt.legend()
        plt.savefig('control_plane_cdf.png')
        plt.close()

        # 4. Control Plane API Calls vs Propagation (Concurrent Only)
        if 'api_calls' in df_cp_conc.columns:
            plt.figure(figsize=(10, 6))
            found_data = df_cp_conc[df_cp_conc['found']]
            plt.scatter(found_data['delay_sec'], found_data['api_calls'], alpha=0.6)
            plt.title('Propagation Time vs API Calls (Concurrent Racer)')
            plt.xlabel('Propagation Time (s)')
            plt.ylabel('API Calls')
            plt.grid(True, alpha=0.3)
            plt.savefig('control_plane_api_calls.png')
            plt.close()

def main():
    print("Loading data from test directories...")
    df_dp_seq = load_csv(DATA_PLANE_SEQ)
    df_cp_seq = load_csv(CONTROL_PLANE_SEQ)
    df_dp_conc = load_csv(DATA_PLANE_CONC)
    df_cp_conc = load_csv(CONTROL_PLANE_CONC)

    with open(OUTPUT_REPORT, "w") as f:
        analyze_data_plane(df_dp_seq, df_dp_conc, f)
        analyze_control_plane(df_cp_seq, df_cp_conc, f)
    
    print(f"Analysis saved to {OUTPUT_REPORT}")

    print("Generating plots...")
    plot_results(df_dp_seq, df_dp_conc, df_cp_seq, df_cp_conc)
    print("Plots saved: data_plane_latency.png, control_plane_propagation.png, control_plane_cdf.png, control_plane_api_calls.png")

if __name__ == "__main__":
    main()
