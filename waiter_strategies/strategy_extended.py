"""
Strategy 2.2: Extended Fixed Timeout

Configuration:
- Polling interval: 3 seconds (fixed)
- Timeout: 600 seconds (10 minutes)
- Purpose: Test if simply waiting longer improves success rate

Trade-off Analysis:
✅ Higher success rate (should cover P99+)
❌ No reduction in API calls
❌ Wastes 10 minutes on genuine failures
❌ Poor user experience for edge cases

This demonstrates that timeout extension alone is insufficient—
we need smarter polling, not just longer waiting.


WITH SIMULATION
"""

from simulated_waiter import SimulatedBaseWaiterStrategy


class ExtendedTimeoutStrategy(SimulatedBaseWaiterStrategy):
    """
    Extended timeout strategy with simulated delays
    """
    
    def __init__(self, bucket_name, enable_simulation=True):
        super().__init__(bucket_name, "Extended-3s-600s", enable_simulation)
        self.polling_interval = 3
        self.timeout_seconds = 600
    
    def calculate_next_interval(self, elapsed_time, check_count):
        return self.polling_interval
    
    def should_timeout(self, elapsed_time, check_count):
        return elapsed_time >= self.timeout_seconds


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python strategy_extended.py <bucket-name>")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    
    print("\n" + "="*70)
    print("STRATEGY 2.2: EXTENDED TIMEOUT TEST (WITH SIMULATION)")
    print("="*70)
    print(f"Bucket: {bucket_name}")
    print(f"Polling: Every 3 seconds")
    print(f"Timeout: 600 seconds (10 minutes)")
    print(f"Simulation: GitHub Issue #25939 distribution")
    print(f"Sample size: 30 iterations")
    print("="*70 + "\n")
    
    strategy = ExtendedTimeoutStrategy(bucket_name, enable_simulation=True)
    results = strategy.run_test_suite(num_tests=30)
    
    print("\nTest suite completed!")


if __name__ == "__main__":
    main()