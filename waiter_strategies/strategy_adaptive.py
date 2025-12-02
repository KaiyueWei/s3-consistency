"""
Strategy 2.3: Adaptive Timeout (Learning from History) - IMPROVED

Configuration:
- Polling interval: Variable (adaptive)
- Timeout: Dynamically predicted based on historical data
- Learning model: Uses past propagation times to predict future needs
- Purpose: Build an empirical model of AWS behavior

Core Innovation: The system learns from experience. If recent 
propagations took 50-60 seconds, predict accordingly. If they 
start taking 150+ seconds, adapt the timeout.

IMPROVEMENTS:
- Increased min_timeout from 180s to 300s (covers P90)
- Changed from 2*stdev to 3*stdev (more conservative, covers tail better)
- Bootstrap period reduced from 5 to 3 tests (learn faster)

Theoretical Foundation: Applies machine learning principles to 
system optimization—addresses the fundamental unpredictability 
of eventual consistency by building an empirical model.

Expected Advantages:
- Reduced API calls through intelligent spacing
- Better timeout coverage through learning
- Adapts to changing AWS behavior patterns

WITH SIMULATION
"""

from simulated_waiter import SimulatedBaseWaiterStrategy
import statistics


class AdaptiveStrategy(SimulatedBaseWaiterStrategy):
    """
    Adaptive strategy with simulated delays - IMPROVED VERSION
    """
    
    def __init__(self, bucket_name, enable_simulation=True):
        super().__init__(bucket_name, "Adaptive-Learning-v2", enable_simulation)
        self.historical_times = []
        self.min_timeout = 300  # ✅ 改进1: 从180s提升到300s (覆盖P90)
        self.max_timeout = 600  
        self.history_size = 20
    
    def calculate_next_interval(self, elapsed_time, check_count):
        """
        ✅ 改进2: 更智能的轮询间隔
        早期更频繁，后期更稀疏（类似Hybrid的思路）
        """
        if elapsed_time < 30:
            return 2  # 早期密集 (原来是3)
        elif elapsed_time < 60:
            return 4  # 中等 (原来是5)
        elif elapsed_time < 120:
            return 8  # 稍疏
        elif elapsed_time < 180:
            return 10  # 稀疏
        else:
            return 15  # 非常稀疏（节省API调用）
    
    def should_timeout(self, elapsed_time, check_count):
        """
        ✅ 改进3: 更保守的timeout计算
        """
        # Bootstrap: 前3个测试用更长的min_timeout（原来是5个）
        if len(self.historical_times) < 3:
            timeout = self.min_timeout
        else:
            mean = statistics.mean(self.historical_times)
            stdev = statistics.stdev(self.historical_times)
            
            # ✅ 改进4: 从2*stdev改成3*stdev（更保守）
            # 2*stdev覆盖95%，3*stdev覆盖99.7%
            timeout = mean + (3 * stdev)
            
            # 确保至少是min_timeout，最多是max_timeout
            timeout = max(self.min_timeout, min(timeout, self.max_timeout))
        
        return elapsed_time >= timeout
    
    def run_single_test(self, test_id):
        result = super().run_single_test(test_id)
        
        if result['success']:
            self.historical_times.append(result['propagation_time'])
            if len(self.historical_times) > self.history_size:
                self.historical_times.pop(0)
        
        return result


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python strategy_adaptive_v2.py <bucket-name>")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    
    print("\n" + "="*70)
    print("STRATEGY 2.3: ADAPTIVE TIMEOUT TEST v2 (IMPROVED)")
    print("="*70)
    print(f"Bucket: {bucket_name}")
    print(f"Polling: Variable (2-15 seconds, adaptive)")
    print(f"Timeout: Dynamic (300-600 seconds, learned)")
    print(f"  - Bootstrap (first 3 tests): 300s minimum")
    print(f"  - Learning phase: mean + 3×stdev")
    print(f"Learning: Based on last 20 successful tests")
    print(f"Simulation: GitHub Issue #25939 distribution")
    print(f"Sample size: 30 iterations")
    print("="*70 + "\n")
    
    strategy = AdaptiveStrategy(bucket_name, enable_simulation=True)
    results = strategy.run_test_suite(num_tests=30)
    
    print("\nTest suite completed!")


if __name__ == "__main__":
    main()