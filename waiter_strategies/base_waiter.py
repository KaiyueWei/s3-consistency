"""
Base Waiter Strategy Framework
Provides common functionality for all waiter strategy implementations
"""
import boto3
import time
import json
import random
from abc import ABC, abstractmethod
from datetime import datetime


class BaseWaiterStrategy(ABC):
    """
    Abstract base class for waiter strategies
    All strategy implementations should inherit from this class
    """
    
    def __init__(self, bucket_name, strategy_name):
        """
        Initialize the waiter strategy
        
        Args:
            bucket_name: S3 bucket name for testing
            strategy_name: Human-readable name for this strategy
        """
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.strategy_name = strategy_name
        self.results = []
    
    @abstractmethod
    def calculate_next_interval(self, elapsed_time, check_count):
        """
        Calculate the next polling interval
        
        Args:
            elapsed_time: Time elapsed since configuration was applied (seconds)
            check_count: Number of checks performed so far
            
        Returns:
            float: Seconds to wait before next check
        """
        pass
    
    @abstractmethod
    def should_timeout(self, elapsed_time, check_count):
        """
        Determine if we should timeout
        
        Args:
            elapsed_time: Time elapsed since configuration was applied (seconds)
            check_count: Number of checks performed so far
            
        Returns:
            bool: True if should timeout, False otherwise
        """
        pass
    
    def generate_lifecycle_config(self, test_id):
        """
        Generate a unique lifecycle configuration for testing
        
        Args:
            test_id: Unique identifier for this test iteration
            
        Returns:
            dict: Lifecycle configuration
        """
        return {
            'Rules': [
                {
                    'ID': f'{self.strategy_name}-rule-{test_id}',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': f'{self.strategy_name}-{test_id}/'},
                    'Expiration': {'Days': 7}
                }
            ]
        }
    
    def check_configuration_match(self, expected_config):
        """
        Check if current S3 lifecycle configuration matches expected
        
        Args:
            expected_config: The lifecycle configuration we expect to see
            
        Returns:
            bool: True if configurations match, False otherwise
        """
        try:
            response = self.s3_client.get_bucket_lifecycle_configuration(
                Bucket=self.bucket_name
            )
            current_rules = response.get('Rules', [])
            expected_rules = expected_config['Rules']
            
            # Check if rule count matches
            if len(current_rules) != len(expected_rules):
                return False
            
            # Check if rule IDs and status match
            for current, expected in zip(current_rules, expected_rules):
                if current.get('ID') != expected.get('ID'):
                    return False
                if current.get('Status') != expected.get('Status'):
                    return False
            
            return True
            
        except self.s3_client.exceptions.NoSuchLifecycleConfiguration:
            return False
        except Exception as e:
            print(f"Error checking configuration: {e}")
            return False
    
    def run_single_test(self, test_id):
        """
        Run a single test iteration with enhanced metrics
        
        Args:
            test_id: Unique identifier for this test
            
        Returns:
            dict: Test result with detailed metrics
        """
        config = self.generate_lifecycle_config(test_id)
        
        # === Phase 1: PUT Request ===
        print(f"  Applying configuration...", end=' ')
        put_start = time.time()
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=config
            )
            put_end = time.time()
            put_duration = put_end - put_start
            print(f"✓ ({put_duration:.3f}s)", end='')
        except Exception as e:
            return {
                'test_id': test_id,
                'success': False,
                'error': f'PUT failed: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }
        
        # === Phase 2: Polling for Propagation ===
        propagation_start = time.time()
        check_count = 0
        api_calls = 1  # Count the PUT operation
        polling_history = []  # Track each polling attempt
        
        print(f"  Waiting", end='')
        
        while True:
            elapsed = time.time() - propagation_start
            
            # Check if we should timeout
            if self.should_timeout(elapsed, check_count):
                print(f" ✗ Timeout after {elapsed:.1f}s")
                return {
                    'test_id': test_id,
                    'success': False,
                    'error': 'Timeout',
                    'put_duration': put_duration,
                    'propagation_time': elapsed,
                    'check_count': check_count,
                    'api_calls': api_calls,
                    'polling_history': polling_history,
                    'timestamp': datetime.now().isoformat()
                }
            
            # Wait before next check
            interval = self.calculate_next_interval(elapsed, check_count)
            time.sleep(interval)
            
            # Perform the check
            check_count += 1
            api_calls += 1
            
            get_start = time.time()
            match = self.check_configuration_match(config)
            get_end = time.time()
            get_duration = get_end - get_start
            
            # Record this polling attempt
            polling_history.append({
                'attempt': check_count,
                'elapsed_at_check': time.time() - propagation_start,
                'get_duration': get_duration,
                'matched': match
            })
            
            if match:
                total_time = time.time() - propagation_start
                print(f" ✓ Success in {total_time:.1f}s ({check_count} checks)")
                return {
                    'test_id': test_id,
                    'success': True,
                    'put_duration': put_duration,
                    'propagation_time': total_time,
                    'check_count': check_count,
                    'api_calls': api_calls,
                    'polling_history': polling_history,
                    'timestamp': datetime.now().isoformat()
                }
    
    def run_test_suite(self, num_tests=30):
        """
        Run the complete test suite
        
        Args:
            num_tests: Number of test iterations to perform
            
        Returns:
            list: All test results
        """
        print(f"\n{'='*60}")
        print(f"Testing Strategy: {self.strategy_name}")
        print(f"{'='*60}")
        print(f"Running {num_tests} test iterations...\n")
        
        for i in range(num_tests):
            print(f"Test {i+1}/{num_tests}:")
            result = self.run_single_test(i)
            self.results.append(result)
            
            # Brief pause between tests to avoid rate limiting
            time.sleep(2)
        
        self.analyze_results()
        self.save_results()
        
        return self.results
    
    def analyze_results(self):
        """
        Analyze and display test results with percentile statistics
        """
        successful = [r for r in self.results if r['success']]
        failed = [r for r in self.results if not r['success']]
        
        print(f"\n{'='*60}")
        print(f"RESULTS: {self.strategy_name}")
        print(f"{'='*60}")
        print(f"Total tests:     {len(self.results)}")
        print(f"Successful:      {len(successful)}")
        print(f"Failed:          {len(failed)}")
        print(f"Success rate:    {(len(successful)/len(self.results))*100:.1f}%")
        
        if successful:
            # Extract metrics
            propagation_times = [r['propagation_time'] for r in successful]
            put_durations = [r['put_duration'] for r in successful]
            api_calls = [r['api_calls'] for r in successful]
            
            # Calculate percentiles
            import numpy as np
            p50 = np.percentile(propagation_times, 50)
            p75 = np.percentile(propagation_times, 75)
            p90 = np.percentile(propagation_times, 90)
            p95 = np.percentile(propagation_times, 95)
            p99 = np.percentile(propagation_times, 99)
            
            print(f"\n📊 Propagation Time Distribution:")
            print(f"  P50 (median):  {p50:.1f}s")
            print(f"  P75:           {p75:.1f}s")
            print(f"  P90:           {p90:.1f}s")
            print(f"  P95:           {p95:.1f}s")
            print(f"  P99:           {p99:.1f}s")
            print(f"  Min:           {min(propagation_times):.1f}s")
            print(f"  Max:           {max(propagation_times):.1f}s")
            print(f"  Mean:          {np.mean(propagation_times):.1f}s")
            print(f"  Std Dev:       {np.std(propagation_times):.1f}s")
            
            print(f"\n⏱️  PUT Request Performance:")
            print(f"  Avg duration:  {np.mean(put_durations):.3f}s")
            print(f"  Min duration:  {min(put_durations):.3f}s")
            print(f"  Max duration:  {max(put_durations):.3f}s")
            
            print(f"\n🔄 API Call Efficiency:")
            print(f"  Avg calls:     {np.mean(api_calls):.1f}")
            print(f"  Min calls:     {min(api_calls)}")
            print(f"  Max calls:     {max(api_calls)}")
            print(f"  Total calls:   {sum(api_calls)}")
        
        self._display_coverage_analysis()
        print(f"{'='*60}\n")
    
    def _display_coverage_analysis(self):
        """
        Display theoretical coverage based on timeout settings
        Based on GitHub Issue #25939 empirical distribution
        """
        # Distribution from GitHub Issue #25939
        distribution = {
            'P50': 48,
            'P75': 82,
            'P85': 120,
            'P90': 142,
            'P95': 156,
            'P99': 234,
            'Max': 312
        }
        
        # Get timeout from strategy
        timeout = getattr(self, 'timeout_seconds', None)
        if timeout is None:
            # For adaptive strategy, use max timeout
            timeout = getattr(self, 'max_timeout', 600)
        
        print(f"\n📈 Theoretical Coverage Analysis (Production):")
        print(f"  Based on GitHub Issue #25939 empirical data")
        print(f"  Strategy timeout: {timeout}s")
        
        # Find coverage percentile
        coverage_percentile = None
        estimated_success_rate = None
        
        for percentile, time_value in sorted(distribution.items(), key=lambda x: x[1]):
            if timeout >= time_value:
                coverage_percentile = percentile
                # Estimate success rate based on percentile
                if percentile == 'P50':
                    estimated_success_rate = 50.0
                elif percentile == 'P75':
                    estimated_success_rate = 75.0
                elif percentile == 'P85':
                    estimated_success_rate = 85.0
                elif percentile == 'P90':
                    estimated_success_rate = 90.0
                elif percentile == 'P95':
                    estimated_success_rate = 95.0
                elif percentile == 'P99':
                    estimated_success_rate = 99.0
                elif percentile == 'Max':
                    estimated_success_rate = 99.9
        
        if coverage_percentile:
            print(f"  Coverage:         {coverage_percentile}")
            print(f"  Expected success: ~{estimated_success_rate:.1f}% (in production)")
            print(f"  Expected failure: ~{100-estimated_success_rate:.1f}% (in production)")
            
            # Warning if coverage is low
            if estimated_success_rate < 95.0:
                print(f"  ⚠️  Warning: Coverage below 95% may result in frequent failures")
        else:
            print(f"  Coverage:         Below P50")
            print(f"  ⚠️  Warning: Timeout too short for production use")
    
    def save_results(self):
        """
        Save test results to JSON file
        """
        filename = f"{self.strategy_name.lower().replace(' ', '_').replace('(', '').replace(')', '')}_results.json"
        with open(filename, 'w') as f:
            json.dump({
                'strategy_name': self.strategy_name,
                'total_tests': len(self.results),
                'successful': len([r for r in self.results if r['success']]),
                'results': self.results
            }, f, indent=2)
        print(f"Results saved to: {filename}")
