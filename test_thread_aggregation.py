#!/usr/bin/env python3
"""
Test script to verify thread count aggregation from JSON files.
This helps debug the thread count issue for multiple TaskManagers.
"""
import json
import sys
from pathlib import Path
import numpy as np

def test_thread_aggregation(json_file_path):
    """Test how thread counts are aggregated from a JSON file."""
    print(f"\n{'='*80}")
    print(f"Testing: {json_file_path}")
    print(f"{'='*80}\n")
    
    if not Path(json_file_path).exists():
        print(f"ERROR: File not found: {json_file_path}")
        return
    
    # Load JSON file (one line per TaskManager)
    all_tm_values = []
    line_num = 0
    
    with open(json_file_path, 'r') as f:
        for line in f:
            line_num += 1
            if line.strip():
                try:
                    data = json.loads(line)
                    values = data.get('values', [])
                    if values:
                        # Filter out None/null values
                        filtered_values = [v for v in values if v is not None]
                        if filtered_values:
                            all_tm_values.append(filtered_values)
                            print(f"Line {line_num}: Found {len(values)} values ({len(filtered_values)} non-null)")
                            print(f"  - Mean: {np.mean(filtered_values):.2f}")
                            print(f"  - Min: {np.min(filtered_values):.2f}, Max: {np.max(filtered_values):.2f}")
                            if len(values) != len(filtered_values):
                                print(f"  - WARNING: Filtered out {len(values) - len(filtered_values)} null values")
                except Exception as e:
                    print(f"Line {line_num}: Error parsing - {e}")
    
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total TaskManagers detected: {len(all_tm_values)}")
    
    if all_tm_values:
        # Calculate aggregation (sum at each time point, then mean)
        min_length = min(len(values) for values in all_tm_values)
        print(f"Minimum time series length: {min_length}")
        
        # Truncate to same length
        aligned_values = [values[:min_length] for values in all_tm_values]
        
        # Sum across TaskManagers at each time point
        summed_series = np.sum(aligned_values, axis=0)
        
        # Take mean of the summed time series
        aggregated_value = np.mean(summed_series)
        
        print(f"\nAggregation Results:")
        print(f"  - Sum at first time point: {summed_series[0]:.2f}")
        print(f"  - Sum at last time point: {summed_series[-1]:.2f}")
        print(f"  - Mean of summed series: {aggregated_value:.2f}")
        print(f"  - Min of summed series: {np.min(summed_series):.2f}")
        print(f"  - Max of summed series: {np.max(summed_series):.2f}")
        
        # Show what the OLD method would have given
        tm_means = [np.mean(values) for values in all_tm_values]
        old_aggregated = np.sum(tm_means)
        print(f"\nOLD method (mean per TM, then sum): {old_aggregated:.2f}")
        print(f"NEW method (sum at each point, then mean): {aggregated_value:.2f}")
        
    else:
        print("No TaskManager data found in file!")
    
    print(f"\n{'='*80}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_thread_aggregation.py <path_to_thread_json_file>")
        print("\nExample:")
        print("  python test_thread_aggregation.py /path/to/a2/1/export/flink_taskmanager_Status_JVM_Threads_Count_export.json")
        sys.exit(1)
    
    test_thread_aggregation(sys.argv[1])
