"""
Strategy 2.1: Baseline (Current Terraform Behavior)

Configuration:
- Polling interval: 3 seconds (fixed)
- Timeout: 180 seconds (3 minutes)
- Purpose: Establish performance baseline

This strategy mimics the current Terraform provider behavior
to establish a baseline for comparison with improved strategies.

WITH SIMULATION
"""

from simulated_waiter import SimulatedBaseWaiterStrategy


class BaselineStrategy(SimulatedBaseWaiterStrategy):
    """
    Baseline strategy with simulated delays
    """
    
    def __init__(self, bucket_name, enable_simulation=True):
        super().__init__(bucket_name, "Baseline-3s-180s", enable_simulation)
        self.polling_interval = 3
        self.timeout_seconds = 180
    
    def calculate_next_interval(self, elapsed_time, check_count):
        return self.polling_interval
    
    def should_timeout(self, elapsed_time, check_count):
        return elapsed_time >= self.timeout_seconds


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python strategy_baseline.py <bucket-name>")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    
    print("\n" + "="*70)
    print("STRATEGY 2.1: BASELINE TEST (WITH SIMULATION)")
    print("="*70)
    print(f"Bucket: {bucket_name}")
    print(f"Polling: Every 3 seconds")
    print(f"Timeout: 180 seconds")
    print(f"Simulation: GitHub Issue #25939 distribution")
    print(f"Sample size: 30 iterations")
    print("="*70 + "\n")
    
    strategy = BaselineStrategy(bucket_name, enable_simulation=True)
    results = strategy.run_test_suite(num_tests=30)
    
    print("\nTest suite completed!")


if __name__ == "__main__":
    main()