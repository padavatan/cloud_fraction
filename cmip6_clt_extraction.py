#!/usr/bin/env python3
"""
CMIP6 Cloud Cover (clt) Data Extractor
-------------------------------------
This script finds and downloads daily cloud cover (clt) data from CMIP6 models
that have matching variants for both historical and ssp585 experiments.

Requirements:
- pyesgf
- pandas
- requests

Install with: pip install pyesgf pandas requests
"""

from pyesgf.search import SearchConnection
import pandas as pd
import os
import time
import sys
from collections import defaultdict

# Configure search parameters
SEARCH_PARAMS = {
    'project': 'CMIP6',
    'variable': 'clt',
    'frequency': 'day',
    'experiments': ['historical', 'ssp585']
}

def get_connection():
    """Connect to ESGF node."""
    return SearchConnection('https://esgf-node.llnl.gov/esg-search', distrib=True)

def find_matching_variants():
    """Find models with matching variants for both historical and ssp585."""
    conn = get_connection()
    
    print("Searching for models with daily cloud cover (clt) data...")
    print("This may take a few minutes. Please be patient.")
    
    # Store results by model and variant
    results_by_exp = {'historical': defaultdict(list), 'ssp585': defaultdict(list)}
    
    # Search for each experiment
    for experiment in SEARCH_PARAMS['experiments']:
        print(f"\nSearching for {experiment} experiment datasets...")
        
        # Create search context
        ctx = conn.new_context(
            project=SEARCH_PARAMS['project'],
            experiment_id=experiment,
            variable=SEARCH_PARAMS['variable'],
            frequency=SEARCH_PARAMS['frequency'],
            latest=True
        )
        
        # Execute search
        results = ctx.search()
        print(f"Found {len(results)} initial {experiment} datasets")
        
        # Process results
        for ds in results:
            try:
                # Extract metadata
                metadata = ds.json['metadata']
                source_id = metadata.get('source_id', [''])[0]  # Model name
                institute = metadata.get('institution_id', [''])[0]
                variant = metadata.get('variant_label', [''])[0]
                grid = metadata.get('grid_label', [''])[0]
                
                model_key = f"{source_id}.{institute}"
                variant_key = f"{variant}.{grid}"
                
                # Store dataset info
                results_by_exp[experiment][model_key].append({
                    'variant': variant,
                    'grid': grid,
                    'variant_key': variant_key,
                    'dataset_id': ds.dataset_id,
                    'model': source_id,
                    'institute': institute
                })
            except Exception as e:
                continue  # Skip problematic entries
    
    # Find models with matching variants
    matched_models = []
    
    for model_key in set(results_by_exp['historical'].keys()) & set(results_by_exp['ssp585'].keys()):
        # Get variants for this model in both experiments
        hist_variants = {v['variant_key']: v for v in results_by_exp['historical'][model_key]}
        ssp_variants = {v['variant_key']: v for v in results_by_exp['ssp585'][model_key]}
        
        # Find common variants
        common_variants = set(hist_variants.keys()) & set(ssp_variants.keys())
        
        for variant_key in common_variants:
            matched_models.append({
                'model': hist_variants[variant_key]['model'],
                'institute': hist_variants[variant_key]['institute'],
                'variant': hist_variants[variant_key]['variant'],
                'grid': hist_variants[variant_key]['grid'],
                'historical_dataset': hist_variants[variant_key]['dataset_id'],
                'ssp585_dataset': ssp_variants[variant_key]['dataset_id']
            })
    
    print(f"\nFound {len(matched_models)} models with matching variants for both experiments")
    return matched_models

def generate_wget_scripts(matched_models, output_dir="./wget_scripts"):
    """Generate wget scripts for each matched model."""
    conn = get_connection()
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Track information for summary
    model_summary = []
    
    for i, model_info in enumerate(matched_models):
        model_name = model_info['model']
        variant = model_info['variant']
        print(f"\nProcessing [{i+1}/{len(matched_models)}] {model_name} {variant}")
        
        model_dir = os.path.join(output_dir, f"{model_name}_{variant}")
        os.makedirs(model_dir, exist_ok=True)
        
        file_counts = {}
        wget_paths = {}
        
        # Process each experiment
        for experiment in ['historical', 'ssp585']:
            dataset_id = model_info[f'{experiment}_dataset']
            
            try:
                # Get dataset
                dataset = conn.search(dataset_id=dataset_id)[0]
                
                # Get file context
                file_ctx = dataset.file_context()
                files = file_ctx.search()
                file_counts[experiment] = len(files)
                
                # Generate wget script
                wget_path = os.path.join(model_dir, f"{experiment}_wget.sh")
                with open(wget_path, 'w') as f:
                    f.write("#!/bin/bash\n\n")
                    f.write(f"# Download script for {model_name} {variant} {experiment}\n")
                    f.write(f"# Dataset: {dataset_id}\n\n")
                    
                    for file in files:
                        url = file.download_url
                        if url:
                            f.write(f"wget '{url}' -O '{file.filename}'\n")
                
                os.chmod(wget_path, 0o755)  # Make executable
                wget_paths[experiment] = wget_path
                
                print(f"  {experiment}: Found {len(files)} files, script saved to {wget_path}")
            
            except Exception as e:
                print(f"  Error processing {experiment} dataset: {e}")
                file_counts[experiment] = 0
        
        # Add to summary
        model_summary.append({
            'model': model_name,
            'institute': model_info['institute'],
            'variant': variant,
            'grid': model_info['grid'],
            'historical_files': file_counts.get('historical', 0),
            'ssp585_files': file_counts.get('ssp585', 0),
            'historical_wget': wget_paths.get('historical', ''),
            'ssp585_wget': wget_paths.get('ssp585', '')
        })
    
    # Create summary file
    summary_df = pd.DataFrame(model_summary)
    summary_path = os.path.join(output_dir, "model_summary.csv")
    summary_df.to_csv(summary_path, index=False)
    
    print(f"\nSummary saved to {summary_path}")
    return summary_df

def main():
    print("CMIP6 Cloud Cover (clt) Data Extractor")
    print("--------------------------------------")
    
    # Find models with matching variants
    matched_models = find_matching_variants()
    
    if not matched_models:
        print("No matching models found. Exiting.")
        return
    
    # Generate wget scripts
    output_dir = "./cmip6_clt_data"
    summary = generate_wget_scripts(matched_models, output_dir)
    
    # Print usage instructions
    print("\nData Extraction Instructions:")
    print("----------------------------")
    print(f"1. Navigate to the output directory: cd {output_dir}")
    print("2. Create directories for each model you want to download")
    print("3. Execute the wget scripts to download data:")
    print("   For historical data: ./model_name_variant/historical_wget.sh")
    print("   For ssp585 data: ./model_name_variant/ssp585_wget.sh")
    print("\nTop 5 models by total file count:")
    
    # Calculate total files and sort
    summary['total_files'] = summary['historical_files'] + summary['ssp585_files']
    top_models = summary.sort_values('total_files', ascending=False).head(5)
    
    for _, row in top_models.iterrows():
        print(f"  {row['model']} {row['variant']}: {row['total_files']} files " 
              f"({row['historical_files']} historical, {row['ssp585_files']} ssp585)")

if __name__ == "__main__":
    main()
