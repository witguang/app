import os
import csv
import numpy as np
from tkinter import messagebox
from typing import List, Tuple, Optional
from config import Config

class FileProcessor:
    """Utility class for processing files."""

    @staticmethod
    def _parse_float(value: str) -> float:
        """Safely converts a string to a float, returning np.nan on failure."""
        return float(value) if value and value.strip() else np.nan

    @staticmethod
    def write_custom_csv(filepath: str, data: list, headers: list):
        """Writes data to a CSV file at a specific path."""
        try:
            with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(data)
        except Exception as e:
            print(f"Error writing custom CSV to {filepath}: {e}")
            messagebox.showerror("File Write Error", f"Could not write custom CSV file.\n\nError: {e}")

    @staticmethod
    def topo_read_thick_file(thick_file_path: str, wafer_id: str, export_thickness_profile: bool = False) -> Tuple[Optional[tuple], Optional[list], Optional[list]]:
        """Reads a Thickness file, extracts data for a given wafer_id, and calculates metrics."""
        try:
            with open(thick_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            header_line_index = next((i for i, line in enumerate(lines) if Config.ThicknessFile.WAFER_ID_COL_NAME in line), None)
            if header_line_index is None:
                return None, None, None

            header = [h.strip() for h in lines[header_line_index].strip().split(",")]
            data_lines = lines[header_line_index + 1:]
            
            wafer_id_col_idx = header.index(Config.ThicknessFile.WAFER_ID_COL_NAME)

            for line in data_lines:
                columns = [c.strip() for c in line.strip().split(",")]
                if len(columns) > wafer_id_col_idx and columns[wafer_id_col_idx] == wafer_id:
                    if len(columns) < Config.ThicknessFile.MIN_REQUIRED_COLS:
                        return None, None, None
                    
                    metrics = FileProcessor._calculate_thickness_metrics(columns, header)
                    profile, zero_profile = None, None
                    if export_thickness_profile:
                        profile, zero_profile = FileProcessor._extract_thickness_profiles(columns, wafer_id)
                    
                    return metrics, profile, zero_profile

            return None, None, None
        except Exception as e:
            print(f"Error reading thick file {thick_file_path}: {e}")
            return None, None, None

    @staticmethod
    def _calculate_thickness_metrics(columns: List[str], header: List[str]) -> Optional[tuple]:
        """Calculates all derived metrics from a single row of thickness data."""
        try:
            C = Config.ThicknessFile
            col_vals = {idx: FileProcessor._parse_float(columns[idx]) for idx in [
                C.COL_609_IDX, C.COL_709_IDX, C.COL_744_IDX, C.COL_749_IDX,
                C.COL_754_IDX, C.COL_756_IDX, C.COL_359_IDX, C.COL_9_IDX,
                C.COL_734_IDX, C.COL_384_IDX
            ]}

            ero147 = col_vals[C.COL_744_IDX] - 1.4 * col_vals[C.COL_709_IDX] + 0.4 * col_vals[C.COL_609_IDX]
            ero148 = col_vals[C.COL_749_IDX] - 1.4 * col_vals[C.COL_709_IDX] + 0.4 * col_vals[C.COL_609_IDX]
            ero149 = col_vals[C.COL_754_IDX] - 1.4 * col_vals[C.COL_709_IDX] + 0.4 * col_vals[C.COL_609_IDX]

            convexity = col_vals[C.COL_9_IDX] - col_vals[C.COL_609_IDX]
            edge = col_vals[C.COL_734_IDX] - col_vals[C.COL_609_IDX]
            center_slope = (col_vals[C.COL_384_IDX] - col_vals[C.COL_9_IDX]) / 75 if col_vals[C.COL_384_IDX] is not np.nan and col_vals[C.COL_9_IDX] is not np.nan else np.nan
            mid_slope = (col_vals[C.COL_609_IDX] - col_vals[C.COL_384_IDX]) / 75 if col_vals[C.COL_609_IDX] is not np.nan and col_vals[C.COL_384_IDX] is not np.nan else np.nan
            
            all_profile_cols = np.array([FileProcessor._parse_float(c) for c in columns[C.PROFILE_START_COL:]])
            
            maxr_value = np.nanmax(all_profile_cols)
            maxr_col_name = header[C.PROFILE_START_COL + np.nanargmax(all_profile_cols)] if not np.all(np.isnan(all_profile_cols)) else ""
            
            maxe_cols = np.array([FileProcessor._parse_float(c) for c in columns[408:]])
            maxe_value = np.nanmax(maxe_cols) - col_vals[C.COL_359_IDX]

            return (
                ero147, ero148, ero149, maxr_col_name, maxe_value,
                convexity, edge, center_slope, mid_slope
            )
        except (ValueError, IndexError) as e:
            print(f"Error calculating metrics for a row: {e}")
            return None

    @staticmethod
    def _extract_thickness_profiles(columns: List[str], wafer_id: str) -> Tuple[List[list], List[list]]:
        """Extracts raw and zeroed thickness profile data from a row."""
        C = Config.ThicknessFile
        start, end = C.PROFILE_START_COL, min(C.PROFILE_END_COL, len(columns))
        
        profile_values = [FileProcessor._parse_float(c) for c in columns[start:end]]
        base_value = profile_values[0] if profile_values and not np.isnan(profile_values[0]) else np.nan

        if not np.isnan(base_value):
            zeroed_values = [v - base_value if not np.isnan(v) else np.nan for v in profile_values]
        else:
            zeroed_values = [np.nan] * len(profile_values)
            
        return [[wafer_id] + profile_values], [[wafer_id] + zeroed_values]

def sanitize_filename(name):
    """Removes illegal characters from a string to be used as a filename."""
    illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    safe_name = name
    for char in illegal_chars:
        safe_name = safe_name.replace(char, '_')
    return safe_name