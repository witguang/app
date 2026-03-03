import pandas as pd
import numpy as np

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
    
    try:
        raw_df = pd.read_csv(input_filename, encoding='utf-8', dtype=str)
    except UnicodeDecodeError:
        raw_df = pd.read_csv(input_filename, encoding='gbk', dtype=str)
        
    raw_df.columns = raw_df.columns.str.strip() 
    total_raw_rows = len(raw_df)
    
    date_col = 'Date'
    device_col = 'Device'
    wafer_col = 'Wafer ID'
    sublot_col = 'Sublot'
    source_slot_col = 'Source Slot'
    time_col = 'Acquisition Time' 

    required_cols = [device_col, wafer_col, date_col, sublot_col, source_slot_col, time_col]
    for col in required_cols:
        if col not in raw_df.columns:
            matches = [c for c in raw_df.columns if c.upper() == col.upper()]
            if matches:
                raw_df.rename(columns={matches[0]: col}, inplace=True)
            else:
                raise KeyError(f"Error: Required column '{col}' not found in file.")
    
    for col in required_cols:
        raw_df[col] = raw_df[col].astype(str).str.strip()

    original_cols = raw_df.columns.tolist()

    log_callback(f"Parsing time for chronology...")
    raw_df['__dt_obj__'] = pd.to_datetime(raw_df[time_col], errors='coerce')
    
    if raw_df['__dt_obj__'].isna().any():
        mask = raw_df['__dt_obj__'].isna()
        failed_count = mask.sum()
        if failed_count < len(raw_df):
            log_callback(f"Note: Using secondary parser for {failed_count} timestamps...")
            raw_df.loc[mask, '__dt_obj__'] = pd.to_datetime(raw_df.loc[mask, time_col], dayfirst=False, errors='coerce')

    raw_df = raw_df.sort_values(by='__dt_obj__').reset_index(drop=True)
    raw_df['__group_id__'] = pd.Series([None] * len(raw_df), dtype='object')
    raw_df['__x_alias__'] = pd.Series([None] * len(raw_df), dtype='object')

    if mode == 'DP Only':
        log_callback("Running DP Only Logic: Processing all data as single stage...")
        df_pre = raw_df.copy()
        df_pre['__group_id__'] = df_pre.index.astype(str)
        df_post = pd.DataFrame(columns=raw_df.columns)
        
        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        final_df = df_pre_renamed.copy()
        final_order = [f'{pre_prefix}{col}' for col in original_cols]
        final_df = final_df[final_df.columns.intersection(final_order + [c for c in final_df.columns if c not in final_order])]
        log_callback(f"Processed {len(final_df)} rows as {pre_label_user}. (No FP/Delta)")

    elif mode == 'Advanced (Cross-Sublot)':
        log_callback(f"Running Advanced Logic: Processing {len(sublot_mappings)} mappings...")
        group_counter = 0
        for idx, (pre_key, post_key, alias_val) in enumerate(sublot_mappings):
            if not pre_key or not post_key:
                log_callback(f"Warning: Skipping empty mapping row {idx+1}")
                continue
            log_callback(f"  > Mapping {idx+1}: '{pre_key}' <-> '{post_key}'. Alias: '{alias_val}'")
            ungrouped_mask = raw_df['__group_id__'].isna()
            mask_pre = raw_df[sublot_col].str.contains(pre_key, case=False, na=False) & ungrouped_mask
            mask_post = raw_df[sublot_col].str.contains(post_key, case=False, na=False) & ungrouped_mask
            
            df_pre_subset = raw_df.loc[mask_pre, [source_slot_col]]
            df_post_subset = raw_df.loc[mask_post, [source_slot_col]]
            if df_pre_subset.empty or df_post_subset.empty:
                log_callback(f"    Skipping: One of the sets is empty.")
                continue

            merge_pairs = pd.merge(df_pre_subset.reset_index(), df_post_subset.reset_index(), on=source_slot_col, how='inner', suffixes=('_pre', '_post'))
            merge_pairs = merge_pairs.drop_duplicates(subset=[source_slot_col])
            merge_pairs = merge_pairs[merge_pairs['index_pre'] != merge_pairs['index_post']]
            
            if merge_pairs.empty:
                log_callback(f"    No matching slots found for this mapping.")
                continue
                
            local_pairs = 0
            for _, row in merge_pairs.iterrows():
                idx_pre = row['index_pre']
                idx_post = row['index_post']
                if pd.isna(raw_df.at[idx_pre, '__group_id__']) and pd.isna(raw_df.at[idx_post, '__group_id__']):
                    group_counter += 1
                    raw_df.at[idx_pre, '__group_id__'] = f"G{group_counter}"
                    raw_df.at[idx_post, '__group_id__'] = f"G{group_counter}"
                    if alias_val:
                        raw_df.at[idx_pre, '__x_alias__'] = alias_val
                        raw_df.at[idx_post, '__x_alias__'] = alias_val
                    local_pairs += 1
            log_callback(f"    Successfully paired {local_pairs} slots.")
        
        raw_df = raw_df.sort_values(by=['__group_id__', '__dt_obj__'])
        raw_df['__seq__'] = raw_df.groupby('__group_id__').cumcount()
        
        df_pre = raw_df[raw_df['__seq__'] == 0].copy()
        df_post = raw_df[raw_df['__seq__'] == 1].copy()

        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        df_post_renamed = df_post.rename(columns={col: f'{post_prefix}{col}' for col in original_cols})

        log_callback(f"Pairing Result: {len(df_pre)} {pre_label_user} vs {len(df_post)} {post_label_user}.")
        merge_cols = ['__group_id__'] + [f'{post_prefix}{col}' for col in original_cols]
        merged_df = pd.merge(df_pre_renamed, df_post_renamed[merge_cols], on='__group_id__', how='left')
        
        final_order = [f'{pre_prefix}{col}' for col in original_cols] + [f'{post_prefix}{col}' for col in original_cols]
        final_order_existing = [col for col in final_order if col in merged_df.columns]
        final_df = merged_df[final_order_existing].copy()
        
        if '__x_alias__' in merged_df.columns:
            target_sublot_col = f'{pre_prefix}{sublot_col}'
            if target_sublot_col in final_df.columns:
                mask_alias = merged_df['__x_alias__'].notna()
                if mask_alias.any():
                    log_callback("Applying custom X-Axis aliases...")
                    final_df.loc[mask_alias, target_sublot_col] = merged_df.loc[mask_alias, '__x_alias__']

    else:
        log_callback("Running Auto Logic: Universal Bridge Pairing...")
        temp_df = raw_df.copy()
        
        temp_df['__key_phys__'] = temp_df[sublot_col] + "_S" + temp_df[source_slot_col] 
        temp_df['__key_logi__'] = temp_df[wafer_col] + "_S" + temp_df[source_slot_col]  

        group_counter = 0

        phys_groups = temp_df.groupby('__key_phys__')
        for _, group in phys_groups:
            if len(group) >= 2:
                indices = group.index
                if pd.isna(temp_df.loc[indices[0], '__group_id__']):
                    group_counter += 1
                    raw_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"
                    temp_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"

        unpaired_mask = raw_df['__group_id__'].isna()
        temp_df.loc[~unpaired_mask, '__group_id__'] = raw_df.loc[~unpaired_mask, '__group_id__']
        
        logi_groups = temp_df[unpaired_mask].groupby('__key_logi__')
        for _, group in logi_groups:
            if len(group) >= 2:
                indices = group.index
                group_counter += 1
                raw_df.loc[[indices[0], indices[-1]], '__group_id__'] = f"G{group_counter}"

        orphan_mask = raw_df['__group_id__'].isna()
        if orphan_mask.any():
            raw_df.loc[orphan_mask, '__group_id__'] = "O" + raw_df.loc[orphan_mask].index.astype(str)

        raw_df = raw_df.sort_values(by=['__group_id__', '__dt_obj__'])
        raw_df['__seq__'] = raw_df.groupby('__group_id__').cumcount()
        
        df_pre = raw_df[raw_df['__seq__'] == 0].copy()
        df_post = raw_df[raw_df['__seq__'] == 1].copy()

        pre_prefix = f"{pre_label_user}_"
        post_prefix = f"{post_label_user}_"
        
        df_pre_renamed = df_pre.rename(columns={col: f'{pre_prefix}{col}' for col in original_cols})
        df_post_renamed = df_post.rename(columns={col: f'{post_prefix}{col}' for col in original_cols})

        log_callback(f"Pairing Result: {len(df_pre)} {pre_label_user} vs {len(df_post)} {post_label_user}.")
        merge_cols = ['__group_id__'] + [f'{post_prefix}{col}' for col in original_cols]
        merged_df = pd.merge(df_pre_renamed, df_post_renamed[merge_cols], on='__group_id__', how='left')
        
        final_order = [f'{pre_prefix}{col}' for col in original_cols] + [f'{post_prefix}{col}' for col in original_cols]
        final_order_existing = [col for col in final_order if col in merged_df.columns]
        final_df = merged_df[final_order_existing].copy()
        
        if '__x_alias__' in merged_df.columns:
            target_sublot_col = f'{pre_prefix}{sublot_col}'
            if target_sublot_col in final_df.columns:
                mask_alias = merged_df['__x_alias__'].notna()
                if mask_alias.any():
                    log_callback("Applying custom X-Axis aliases...")
                    final_df.loc[mask_alias, target_sublot_col] = merged_df.loc[mask_alias, '__x_alias__']

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

    if mode == 'Auto':
        log_callback("Performing Mode 1 Specific Cleaning & Statistics...")
        post_dev_col = f'{post_prefix}{device_col}'
        pre_dev_col = f'{pre_prefix}{device_col}'
        is_orphan = final_df[post_dev_col].isna()
        df_orphans = final_df[is_orphan]
        df_valid = final_df[~is_orphan]
        
        target_devs = ["FPMS004", "FPMS007"]
        if pre_dev_col in df_orphans.columns:
            orphan_devs = df_orphans[pre_dev_col].astype(str)
            mask_orphan_dp = orphan_devs.isin(target_devs)
            count_orphan_dp = mask_orphan_dp.sum()
            mask_orphan_fp = ~mask_orphan_dp
            count_orphan_fp = mask_orphan_fp.sum()
        else:
            count_orphan_dp = 0
            count_orphan_fp = len(df_orphans)
        
        final_df = df_valid.copy()
        
        del_maxe_col = 'DEL_MaxE'
        if del_maxe_col in final_df.columns:
            count_delta_ok = final_df[del_maxe_col].notna().sum()
        else:
            delta_cols = [c for c in final_df.columns if c.startswith('DEL_')]
            if delta_cols:
                count_delta_ok = final_df[delta_cols[0]].notna().sum()
            else:
                count_delta_ok = len(final_df) 
                
        if actual_data_cols:
            delta_cols_all = [f'DEL_{c}' for c in actual_data_cols if f'DEL_{c}' in final_df.columns]
            if delta_cols_all:
                has_nan = final_df[delta_cols_all].isna().any(axis=1)
                count_partial = has_nan.sum()
            else:
                count_partial = 0
        else:
            count_partial = 0

        stats_msg = (
            f"\n--- [Mode 1 Statistics] ---\n"
            f"1. Total Original Rows: {total_raw_rows}\n"
            f"2. Valid Delta Count (Pairs): {count_delta_ok}\n"
            f"3. Deleted Orphan DP (FPMS004/007): {count_orphan_dp}\n"
            f"4. Deleted Orphan FP (Others): {count_orphan_fp}\n"
            f"5. Partial/ERO Data (Kept): {count_partial}\n"
            f"---------------------------"
        )
        try:
            log_callback(stats_msg, to_file=True)
        except TypeError:
             log_callback(stats_msg)
    
    return final_df, original_cols, actual_data_cols, pre_label_user, post_label_user