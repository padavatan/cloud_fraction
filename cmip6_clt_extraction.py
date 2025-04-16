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
    """Find models with matching variants for both historical and ssp585.
    Searches ssp585 first since it typically has fewer available models."""
    conn = get_connection()
    
    print("Searching for models with daily cloud cover (clt) data...")
    print("This may take a few minutes. Please be patient.")
    
    # Define facets of interest for the search (resolves the warning)
    facets = 'project,experiment_id,source_id,institution_id,variant_label,grid_label,frequency,variable'
    
    # Store all matched models by experiment
    ssp585_models = {}
    historical_models = {}
    
    # OPTIMIZED ORDER: Search for ssp585 experiment datasets FIRST since they're fewer
    print("\nSearching for ssp585 experiment datasets...")
    ssp_ctx = conn.new_context(
        facets=facets,
        project=SEARCH_PARAMS['project'],
        experiment_id='ssp585',
        variable=SEARCH_PARAMS['variable'],
        frequency=SEARCH_PARAMS['frequency'],
        latest=True
    )
    
    # Execute ssp585 search
    ssp_results = ssp_ctx.search()
    print(f"Found {len(ssp_results)} initial ssp585 datasets")
    
    # Process ssp585 results
    print("Processing ssp585 datasets...")
    
    # Keep track of models we've seen to avoid duplicates
    seen_models = set()
    
    for ds in ssp_results:
        try:
            # Extract metadata
            source_id = ds.json.get('source_id', [''])[0]  # Model name
            institute = ds.json.get('institution_id', [''])[0]
            variant = ds.json.get('variant_label', [''])[0]
            grid = ds.json.get('grid_label', [''])[0]
            
            # Create a unique identifier for this model
            model_key = f"{source_id}"
            
            # Skip if we've already seen this model (to ensure uniqueness)
            if model_key in seen_models:
                continue
                
            # Mark this model as seen
            seen_models.add(model_key)
            
            # Create a unique identifier for this model/variant combination
            model_variant_key = f"{source_id}_{variant}_{grid}"
            
            # Store dataset info
            ssp585_models[model_variant_key] = {
                'variant': variant,
                'grid': grid,
                'dataset_id': ds.dataset_id,
                'model': source_id,
                'institute': institute
            }
        except Exception as e:
            print(f"Error processing ssp585 dataset: {e}")
            continue  # Skip problematic entries
    
    print(f"Processed {len(ssp585_models)} unique model-variant combinations for ssp585 experiment")
    
    # Now search for historical experiment datasets for the models we found in ssp585
    print("\nSearching for historical experiment datasets...")
    
    # Reset seen models
    seen_models = set()
    
    # Get the list of models we have in ssp585
    ssp_models_list = list(set(info['model'] for info in ssp585_models.values()))
    print(f"Searching for historical data from {len(ssp_models_list)} models found in ssp585")
    
    hist_ctx = conn.new_context(
        facets=facets,
        project=SEARCH_PARAMS['project'],
        experiment_id='historical',
        variable=SEARCH_PARAMS['variable'],
        frequency=SEARCH_PARAMS['frequency'],
        latest=True
    )
    
    # Execute historical search
    hist_results = hist_ctx.search()
    print(f"Found {len(hist_results)} initial historical datasets")
    
    # Process historical results
    print("Processing historical datasets...")
    for ds in hist_results:
        try:
            # Extract metadata
            source_id = ds.json.get('source_id', [''])[0]  # Model name
            
            # Skip if this model isn't in our ssp585 results
            if source_id not in ssp_models_list:
                continue
                
            institute = ds.json.get('institution_id', [''])[0]
            variant = ds.json.get('variant_label', [''])[0]
            grid = ds.json.get('grid_label', [''])[0]
            
            # Create a unique identifier for this model
            model_key = f"{source_id}"
            
            # Skip if we've already seen this model (to ensure uniqueness)
            if model_key in seen_models:
                continue
                
            # Mark this model as seen
            seen_models.add(model_key)
            
            # Create a unique identifier for this model/variant combination
            model_variant_key = f"{source_id}_{variant}_{grid}"
            
            # Store dataset info
            historical_models[model_variant_key] = {
                'variant': variant,
                'grid': grid,
                'dataset_id': ds.dataset_id,
                'model': source_id,
                'institute': institute
            }
        except Exception as e:
            print(f"Error processing historical dataset: {e}")
            continue  # Skip problematic entries
    
    print(f"Processed {len(historical_models)} unique model-variant combinations for historical experiment")
    
    # Find common model-variant combinations
    common_keys = set(historical_models.keys()) & set(ssp585_models.keys())
    
    # Build matched models list, ensuring one variant per model
    matched_models = []
    already_included_models = set()
    
    print("\nFinding matching model variants between experiments...")
    for key in common_keys:
        hist_info = historical_models[key]
        ssp_info = ssp585_models[key]
        
        # Skip if we've already included this model
        model_name = hist_info['model']
        if model_name in already_included_models:
            continue
            
        # Mark this model as included
        already_included_models.add(model_name)
        
        matched_models.append({
            'model': hist_info['model'],
            'institute': hist_info['institute'],
            'variant': hist_info['variant'],
            'grid': hist_info['grid'],
            'historical_dataset': hist_info['dataset_id'],
            'ssp585_dataset': ssp_info['dataset_id']
        })
    
    print(f"Found {len(matched_models)} unique models with matching variants for both experiments")
    
    # Print some examples of matched models for verification
    if matched_models:
        print("\nExample matches:")
        for i, model in enumerate(matched_models[:5]):  # Show up to 5 examples
            print(f"  {i+1}. {model['model']} {model['variant']} ({model['institute']})")
        if len(matched_models) > 5:
            print(f"  ... and {len(matched_models)-5} more")
    
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
        print("\nPossible reasons for no matches:")
        print("1. The search criteria might be too restrictive")
        print("2. There may be no models with exactly matching variants across both experiments")
        print("3. The ESGF index might be temporarily unavailable or incomplete")
        print("\nTry these fixes:")
        print("1. Check ESGF web portal to verify data availability: https://esgf-node.llnl.gov/search/cmip6/")
        print("2. Try relaxing the search criteria, e.g., search for monthly instead of daily data")
        print("3. Try again later as ESGF indexes are periodically updated")
        return
    
    # Generate wget scripts
    output_dir = "./cmip6_clt_data"
    summary = generate_wget_scripts(matched_models, output_dir)
    
    # Print usage instructions
    print("\nData Extraction Instructions:")
    print("----------------------------")
    print(f"1. Navigate to the output directory: cd {output_dir}")
    print("2. Execute the wget scripts to download data:")
    print("   For historical data: ./model_name_variant/historical_wget.sh")
    print("   For ssp585 data: ./model_name_variant/ssp585_wget.sh")
    print("3. You may need to create an ESGF account and add credentials for datasets requiring authentication")
    print("\nTop 5 models by total file count:")
    
    # Calculate total files and sort
    summary['total_files'] = summary['historical_files'] + summary['ssp585_files']
    top_models = summary.sort_values('total_files', ascending=False).head(5)
    
    for _, row in top_models.iterrows():
        print(f"  {row['model']} {row['variant']}: {row['total_files']} files " 
              f"({row['historical_files']} historical, {row['ssp585_files']} ssp585)")
    
    print("\nNote: For large models with many files, you may want to subset data by time period")
    print("or geographical region before downloading. Consider using OPeNDAP for this purpose.")

if __name__ == "__main__":
    main()
