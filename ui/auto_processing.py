import re

import numpy as np
import pandas as pd

from config import Config

def sanitize_filename(name):
    """Removes illegal characters from a string to be used as a filename."""
    illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    safe_name = name
    for char in illegal_chars:
        safe_name = safe_name.replace(char, '_')
    return safe_name

# -----------------------------------------------------------------------------
# Core Data Processing Logic (Added for DataReportTool)
# -----------------------------------------------------------------------------
def process_and_clean_data_final(input_filename, log_callback, config):
    """
    Reads, cleans, processes, and calculates DELTA values with universal compatibility.
    config dict contains: 'mode', 'sublot_mappings' (list of tuples), 'pre_label', 'post_label'
    """
    mode = config.get('mode', 'Auto')
    pre_label_user = config.get('pre_label', 'DP')
    post_label_user = config.get('post_label', 'FP')
    sublot_mappings = config.get('sublot_mappings', [])

    log_callback(f"Reading file... (Mode: {mode})")
    
    # 1. Read and Normalize Headers
    try:
        raw_df = pd.read_csv(input_filename, encoding='utf-8', dtype=str)
    except UnicodeDecodeError:
        raw_df = pd.read_csv(input_filename, encoding='gbk', dtype=str)
        
    raw_df.columns = raw_df.columns.str.strip() 
    
    total_raw_rows = len(raw_df) # Capture initial count
    
    date_col = 'Date'
    device_col = 'Device'
    wafer_col = 'Wafer ID'
    sublot_col = 'Sublot'
    source_slot_col = 'Source Slot'
    time_col = 'Acquisition Time' 

    required_cols = [device_col, wafer_col, date_col, sublot_col, source_slot_col, time_col]
    for col in required_cols:
        if col not in raw_df.columns:
            # Attempt case-insensitive matching if direct match fails
            matches = [c for c in raw_df.columns if c.upper() == col.upper()]
            if matches:
                raw_df.rename(columns={matches[0]: col}, inplace=True)
            else:
                # Try soft fail or create dummy if non-critical, but these seem critical
                raise KeyError(f"Error: Required column '{col}' not found in file.")
    
    for col in required_cols:
        raw_df[col] = raw_df[col].astype(str).str.strip()

    original_cols = raw_df.columns.tolist()

    # --- 1. Robust Time Parsing ---
    log_callback(f"Parsing time for chronology...")
    raw_df['__dt_obj__'] = pd.to_datetime(raw_df[time_col], errors='coerce')
    
    if raw_df['__dt_obj__'].isna().any():
        mask = raw_df['__dt_obj__'].isna()
        failed_count = mask.sum()
        if failed_count < len(raw_df):
            log_callback(f"Note: Using secondary parser for {failed_count} timestamps...")
            raw_df.loc[mask, '__dt_obj__'] = pd.to_datetime(raw_df.loc[mask, time_col], dayfirst=False, errors='coerce')

    # Global sort by time
    raw_df = raw_df.sort_values(by='__dt_obj__').reset_index(drop=True)
    
    # Initialize Group ID with explicit object type
    raw_df['__group_id__'] = pd.Series([None] * len(raw_df), dtype='object')
    
    # Initialize Alias column for Mode 2
    raw_df['__x_alias__'] = pd.Series([None] * len(raw_df), dtype='object')

    # ==========================================
    # PAIRING LOGIC BRANCHING
    # ==========================================
    
    if mode == 'DP Only':
        # ==========================================
        # MODE 3: DP Only (No Pairing)
        # ==========================================
        log_callback("Running DP Only Logic: Processing all data as single stage...")
        
        # Treat all data as 'Pre' (DP)
        df_pre = raw_df.copy()
        # Create a dummy Group ID just to keep structure valid if needed
        df_pre['__group_id__'] = df_pre.index.astype(str)
        
        # Post df is empty
        df_post = pd.DataFrame(columns=raw_df.columns)
        
        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        
        # Final DF is just Pre data
        final_df = df_pre_renamed.copy()
        final_order = [f'{pre_prefix}{col}' for col in original_cols]
        # Reorder/filter columns to match standard output structure (ignoring missing post cols)
        final_df = final_df[final_df.columns.intersection(final_order + [c for c in final_df.columns if c not in final_order])]
        
        log_callback(f"Processed {len(final_df)} rows as {pre_label_user}. (No FP/Delta)")

    elif mode == 'Advanced (Cross-Sublot)':
        log_callback(f"Running Advanced Logic: Processing {len(sublot_mappings)} mappings...")
        
        group_counter = 0
        
        # Iterate through each user-defined pair (Pre_Key, Post_Key, Alias)
        for idx, (pre_key, post_key, alias_val) in enumerate(sublot_mappings):
            if not pre_key or not post_key:
                log_callback(f"Warning: Skipping empty mapping row {idx+1}")
                continue
                
            log_callback(f"  > Mapping {idx+1}: '{pre_key}' <-> '{post_key}'. Alias: '{alias_val}'")
            
            # Check for ungrouped rows
            ungrouped_mask = raw_df['__group_id__'].isna()
            
            # 1. Identify distinct sets of rows for Pre and Post
            mask_pre = raw_df[sublot_col].str.contains(pre_key, case=False, na=False) & ungrouped_mask
            mask_post = raw_df[sublot_col].str.contains(post_key, case=False, na=False) & ungrouped_mask
            
            df_pre_subset = raw_df.loc[mask_pre, [source_slot_col]]
            df_post_subset = raw_df.loc[mask_post, [source_slot_col]]
            
            if df_pre_subset.empty or df_post_subset.empty:
                log_callback(f"    Skipping: One of the sets is empty.")
                continue

            # 2. Strict Merge Pairing on Slot
            merge_pairs = pd.merge(
                df_pre_subset.reset_index(), 
                df_post_subset.reset_index(), 
                on=source_slot_col, 
                how='inner', 
                suffixes=('_pre', '_post')
            )
            
            # Remove duplicates to ensure 1-to-1 pairing per slot
            merge_pairs = merge_pairs.drop_duplicates(subset=[source_slot_col])
            # Prevent self-pairing
            merge_pairs = merge_pairs[merge_pairs['index_pre'] != merge_pairs['index_post']]
            
            if merge_pairs.empty:
                log_callback(f"    No matching slots found for this mapping.")
                continue
                
            # 3. Assign Group IDs and Alias
            local_pairs = 0
            for _, row in merge_pairs.iterrows():
                idx_pre = row['index_pre']
                idx_post = row['index_post']
                
                # Double check availability
                if pd.isna(raw_df.at[idx_pre, '__group_id__']) and pd.isna(raw_df.at[idx_post, '__group_id__']):
                    group_counter += 1
                    raw_df.at[idx_pre, '__group_id__'] = f"G{group_counter}"
                    raw_df.at[idx_post, '__group_id__'] = f"G{group_counter}"
                    
                    # Store the user defined alias if provided
                    if alias_val:
                        raw_df.at[idx_pre, '__x_alias__'] = alias_val
                        raw_df.at[idx_post, '__x_alias__'] = alias_val
                        
                    local_pairs += 1
            
            log_callback(f"    Successfully paired {local_pairs} slots.")
        
        # --- Separation for Advanced Mode ---
        # Sort by GroupID then Time
        raw_df = raw_df.sort_values(by=['__group_id__', '__dt_obj__'])
        raw_df['__seq__'] = raw_df.groupby('__group_id__').cumcount()
        
        df_pre = raw_df[raw_df['__seq__'] == 0].copy()
        df_post = raw_df[raw_df['__seq__'] == 1].copy()

        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        df_post_renamed = df_post.rename(columns={col: f'{post_prefix}{col}' for col in original_cols})

        log_callback(f"Pairing Result: {len(df_pre)} {pre_label_user} vs {len(df_post)} {post_label_user}.")

        # --- Merge ---
        # We also bring in __x_alias__ from pre (it matches post anyway)
        merge_cols = ['__group_id__'] + [f'{post_prefix}{col}' for col in original_cols]
        
        merged_df = pd.merge(
            df_pre_renamed, 
            df_post_renamed[merge_cols], 
            on='__group_id__', 
            how='left'
        )
        
        final_order = [f'{pre_prefix}{col}' for col in original_cols] + [f'{post_prefix}{col}' for col in original_cols]
        final_order_existing = [col for col in final_order if col in merged_df.columns]
        final_df = merged_df[final_order_existing].copy()
        
        # --- Alias Application (Override Sublot Column for X-Axis) ---
        if '__x_alias__' in merged_df.columns:
            # Where alias is not null, override the Sublot column
            target_sublot_col = f'{pre_prefix}{sublot_col}'
            if target_sublot_col in final_df.columns:
                mask_alias = merged_df['__x_alias__'].notna()
                # We use values from merged_df to update final_df
                if mask_alias.any():
                    log_callback("Applying custom X-Axis aliases...")
                    # Ensure index alignment
                    final_df.loc[mask_alias, target_sublot_col] = merged_df.loc[mask_alias, '__x_alias__']

    else:
        # ==========================================
        # MODE 1: AUTO (Universal Pairing)
        # ==========================================
        log_callback("Running Auto Logic: Universal Bridge Pairing...")
        temp_df = raw_df.copy()
        
        temp_df['__key_phys__'] = temp_df[sublot_col] + "_S" + temp_df[source_slot_col] 
        temp_df['__key_logi__'] = temp_df[wafer_col] + "_S" + temp_df[source_slot_col]  

        group_counter = 0

        # PRIORITY 1: Physical Bridge
        phys_groups = temp_df.groupby('__key_phys__')
        for _, group in phys_groups:
            if len(group) >= 2:
                indices = group.index
                if pd.isna(temp_df.loc[indices[0], '__group_id__']):
                    group_counter += 1
                    raw_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"
                    temp_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"

        # PRIORITY 2: Logical Bridge
        unpaired_mask = raw_df['__group_id__'].isna()
        temp_df.loc[~unpaired_mask, '__group_id__'] = raw_df.loc[~unpaired_mask, '__group_id__']
        
        logi_groups = temp_df[unpaired_mask].groupby('__key_logi__')
        for _, group in logi_groups:
            if len(group) >= 2:
                indices = group.index
                group_counter += 1
                raw_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"

        # Label Orphans
        orphan_mask = raw_df['__group_id__'].isna()
        if orphan_mask.any():
            raw_df.loc[orphan_mask, '__group_id__'] = "O" + raw_df.loc[orphan_mask].index.astype(str)

        # --- Separation ---
        # Sort by GroupID then Time
        raw_df = raw_df.sort_values(by=['__group_id__', '__dt_obj__'])
        raw_df['__seq__'] = raw_df.groupby('__group_id__').cumcount()
        
        df_pre = raw_df[raw_df['__seq__'] == 0].copy()
        df_post = raw_df[raw_df['__seq__'] == 1].copy()

        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        df_post_renamed = df_post.rename(columns={col: f'{post_prefix}{col}' for col in original_cols})

        log_callback(f"Pairing Result: {len(df_pre)} {pre_label_user} vs {len(df_post)} {post_label_user}.")

        # --- Merge ---
        # We also bring in __x_alias__ from pre (it matches post anyway)
        merge_cols = ['__group_id__'] + [f'{post_prefix}{col}' for col in original_cols]
        
        merged_df = pd.merge(
            df_pre_renamed, 
            df_post_renamed[merge_cols], 
            on='__group_id__', 
            how='left'
        )
        
        final_order = [f'{pre_prefix}{col}' for col in original_cols] + [f'{post_prefix}{col}' for col in original_cols]
        final_order_existing = [col for col in final_order if col in merged_df.columns]
        final_df = merged_df[final_order_existing].copy()
        
        # --- Alias Application (Override Sublot Column for X-Axis) ---
        if '__x_alias__' in merged_df.columns:
            # Where alias is not null, override the Sublot column
            target_sublot_col = f'{pre_prefix}{sublot_col}'
            if target_sublot_col in final_df.columns:
                mask_alias = merged_df['__x_alias__'].notna()
                # We use values from merged_df to update final_df
                if mask_alias.any():
                    log_callback("Applying custom X-Axis aliases...")
                    # Ensure index alignment
                    final_df.loc[mask_alias, target_sublot_col] = merged_df.loc[mask_alias, '__x_alias__']

    # --- Data Column Detection ---
    potential_data_cols = [c for c in original_cols if c not in required_cols and c != date_col]
    actual_data_cols = []
    
    pre_prefix = f"{pre_label_user}_"
    post_prefix = f"{post_label_user}_"

    for base_col in potential_data_cols:
        col_p = f'{pre_prefix}{base_col}'
        col_f = f'{post_prefix}{base_col}'
        
        if col_p in final_df.columns:
            final_df[col_p] = pd.to_numeric(final_df[col_p], errors='coerce')
            if final_df[col_p].notna().sum() > 0:
                actual_data_cols.append(base_col)
        
        if mode != 'DP Only' and col_f in final_df.columns:
            final_df[col_f] = pd.to_numeric(final_df[col_f], errors='coerce')

    # --- Delta Calculation (Skipped for Mode 3) ---
    if mode != 'DP Only' and not df_post.empty:
        log_callback(f"Calculating Delta ({len(actual_data_cols)} params)...")
        for base_col in actual_data_cols:
            col_p = f'{pre_prefix}{base_col}'
            col_f = f'{post_prefix}{base_col}'
            del_col = f'DEL_{base_col}'
            
            if col_p in final_df.columns and col_f in final_df.columns:
                if "Thickness" in base_col:
                    final_df[del_col] = final_df[col_p] - final_df[col_f]
                else:
                    final_df[del_col] = final_df[col_f] - final_df[col_p]

    # ==========================================
    # STATISTICS & CLEANING (Mode 1 Specific)
    # ==========================================
    if mode == 'Auto':
        log_callback("Performing Mode 1 Specific Cleaning & Statistics...")
        
        # Define Columns for checking
        post_dev_col = f'{post_prefix}{device_col}'
        pre_dev_col = f'{pre_prefix}{device_col}'
        
        # 1. Identify Orphans (Rows where Post-side Device is missing/NaN)
        # Note: 'how=left' merge ensures Pre exists, but Post might be NaN
        is_orphan = final_df[post_dev_col].isna()
        
        # Split into Orphans vs Paired
        df_orphans = final_df[is_orphan]
        df_valid = final_df[~is_orphan]
        
        # 2. Categorize Orphans
        # Type 1: Orphan DP (Pre Device is FPMS004 or FPMS007) -> DELETE
        target_devs = ["FPMS004", "FPMS007"]
        # Ensure string comparison
        if pre_dev_col in df_orphans.columns:
            orphan_devs = df_orphans[pre_dev_col].astype(str)
            mask_orphan_dp = orphan_devs.isin(target_devs)
            count_orphan_dp = mask_orphan_dp.sum()
            
            # Type 2: Orphan FP (Pre Device is NOT FPMS004/007) -> DELETE
            # (These are likely FPs that ended up in the DP slot because they had no pair)
            mask_orphan_fp = ~mask_orphan_dp
            count_orphan_fp = mask_orphan_fp.sum()
        else:
            count_orphan_dp = 0
            count_orphan_fp = len(df_orphans)
        
        # 3. Apply Deletion (Keep only valid pairs)
        final_df = df_valid.copy()
        
        # 4. Count Delta / ERO / Partial
        # Count rows with valid Delta (e.g., using MaxE or first data column)
        # "delta count (rows where delta_MaxE has value)"
        del_maxe_col = 'DEL_MaxE'
        if del_maxe_col in final_df.columns:
            count_delta_ok = final_df[del_maxe_col].notna().sum()
        else:
            # Fallback: check first available DEL column or just row count
            delta_cols = [c for c in final_df.columns if c.startswith('DEL_')]
            if delta_cols:
                count_delta_ok = final_df[delta_cols[0]].notna().sum()
            else:
                count_delta_ok = len(final_df) # Should technically be all valid pairs
                
        # Count "Partial/ERO"
        # "Rows with some missing data, usually starts with ERO147"
        # We define this as rows in the FINAL set (non-deleted) that have missing Delta values
        if actual_data_cols:
            delta_cols_all = [f'DEL_{c}' for c in actual_data_cols if f'DEL_{c}' in final_df.columns]
            if delta_cols_all:
                # Rows where ANY delta column is NaN
                has_nan = final_df[delta_cols_all].isna().any(axis=1)
                count_partial = has_nan.sum()
            else:
                count_partial = 0
        else:
            count_partial = 0

        # Log Report
        stats_msg = (
            f"\n--- [Mode 1 Statistics] ---\n"
            f"1. Total Original Rows: {total_raw_rows}\n"
            f"2. Valid Delta Count (Pairs): {count_delta_ok}\n"
            f"3. Deleted Orphan DP (FPMS004/007): {count_orphan_dp}\n"
            f"4. Deleted Orphan FP (Others): {count_orphan_fp}\n"
            f"5. Partial/ERO Data (Kept): {count_partial}\n"
            f"---------------------------"
        )
        # ONLY log this to file (as well as screen)
        # To make this work without erroring if 'to_file' argument is not supported by standard print, 
        # we rely on the log_callback implementation in DataReportFunction
        try:
            log_callback(stats_msg, to_file=True)
        except TypeError:
             # Fallback for old loggers
             log_callback(stats_msg)
    
    return final_df, original_cols, actual_data_cols, pre_label_user, post_label_user

