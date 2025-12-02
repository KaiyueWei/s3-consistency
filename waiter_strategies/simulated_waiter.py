"""
Simulated S3 Waiter for Testing in Learner Lab
Injects realistic propagation delays based on GitHub Issue #25939 data
"""

import random
import time
from base_waiter import BaseWaiterStrategy


class SimulatedBaseWaiterStrategy(BaseWaiterStrategy):
    """
    Base waiter with simulated eventual consistency delays
    """
    
    def __init__(self, bucket_name, strategy_name, enable_simulation=True):
        super().__init__(bucket_name, strategy_name)
        self.enable_simulation = enable_simulation
        # Empirical distribution from GitHub Issue #25939
        self.delay_distribution = {
            'P50': 48,   # 50% of requests
            'P75': 82,   # 75% of requests
            'P85': 120,  # 85% of requests
            'P90': 142,  # 90% of requests
            'P95': 156,  # 95% of requests
            'P99': 234,  # 99% of requests
            'Max': 312   # Maximum observed
        }
    
    def _generate_propagation_delay(self):
        """
        Generate realistic propagation delay based on percentile distribution
        """
        rand = random.random()
        
        if rand < 0.50:
            # P0-P50: 0-48 seconds
            return random.uniform(0, 48)
        elif rand < 0.75:
            # P50-P75: 48-82 seconds
            return random.uniform(48, 82)
        elif rand < 0.85:
            # P75-P85: 82-120 seconds
            return random.uniform(82, 120)
        elif rand < 0.90:
            # P85-P90: 120-142 seconds
            return random.uniform(120, 142)
        elif rand < 0.95:
            # P90-P95: 142-156 seconds
            return random.uniform(142, 156)
        elif rand < 0.99:
            # P95-P99: 156-234 seconds
            return random.uniform(156, 234)
        else:
            # P99-Max: 234-312 seconds
            return random.uniform(234, 312)
    
    def check_configuration_match(self, expected_config):
        """
        Override to simulate eventual consistency with realistic delays
        """
        if not self.enable_simulation:
            # Skip simulation, use real AWS behavior
            return super().check_configuration_match(expected_config)
        
        # Generate delay on first check
        if not hasattr(self, '_simulated_delay'):
            self._simulated_delay = self._generate_propagation_delay()
            self._propagation_start = time.time()
            print(f" [Sim: {self._simulated_delay:.1f}s]", end='')
        
        # Check if enough time has passed
        elapsed = time.time() - self._propagation_start
        if elapsed >= self._simulated_delay:
            # Propagation complete, do real check
            return super().check_configuration_match(expected_config)
        else:
            # Still propagating
            return False
    
    def run_single_test(self, test_id):
        """
        Reset simulation state for each test
        """
        if hasattr(self, '_simulated_delay'):
            delattr(self, '_simulated_delay')
        if hasattr(self, '_propagation_start'):
            delattr(self, '_propagation_start')
        return super().run_single_test(test_id)