"""
Strategy 2.4: Hybrid (Early-Dense, Late-Sparse Polling)

Configuration:
- Adaptive polling interval:
  * First 30 seconds: Check frequently (dense monitoring)
  * 30-60 seconds: Moderate checking
  * 60-120 seconds: Less frequent checking
  * 120+ seconds: Sparse checking
- Timeout: 600 seconds
- Purpose: Optimize for common case while handling edge cases

Rationale: Inspired by Dean & Barroso's hedging strategies:
- Most propagations complete quickly → dense early checks capture these
- Rare slow propagations exist → sparse late checks accommodate without waste

This strategy balances efficiency (few API calls for fast propagations) 
with reliability (handles slow propagations gracefully).

WITH SIMULATION
"""

from simulated_waiter import SimulatedBaseWaiterStrategy


class HybridStrategy(SimulatedBaseWaiterStrategy):
    """
    Hybrid strategy with simulated delays
    """
    
    def __init__(self, bucket_name, enable_simulation=True):
        super().__init__(bucket_name, "Hybrid-Dense-Sparse", enable_simulation)
        self.timeout_seconds = 600
    
    def calculate_next_interval(self, elapsed_time, check_count):
        if elapsed_time < 30:
            return 2
        elif elapsed_time < 60:
            return 4
        elif elapsed_time < 120:
            return 8
        else:
            return 15
    
    def should_timeout(self, elapsed_time, check_count):
        return elapsed_time >= self.timeout_seconds


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python strategy_hybrid.py <bucket-name>")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    
    print("\n" + "="*70)
    print("STRATEGY 2.4: HYBRID POLLING TEST (WITH SIMULATION)")
    print("="*70)
    print(f"Bucket: {bucket_name}")
    print(f"Polling: Adaptive (2-15 seconds)")
    print(f"  - 0-30s: Every 2 seconds (dense)")
    print(f"  - 30-60s: Every 4 seconds (moderate)")
    print(f"  - 60-120s: Every 8 seconds (less frequent)")
    print(f"  - 120+s: Every 15 seconds (sparse)")
    print(f"Timeout: 600 seconds (10 minutes)")
    print(f"Simulation: GitHub Issue #25939 distribution")
    print(f"Sample size: 30 iterations")
    print("="*70 + "\n")
    
    strategy = HybridStrategy(bucket_name, enable_simulation=True)
    results = strategy.run_test_suite(num_tests=30)
    
    print("\nTest suite completed!")


if __name__ == "__main__":
    main()