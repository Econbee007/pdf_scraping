import pdfplumber
import pandas as pd
import os
import re
from pathlib import Path
from collections import defaultdict

# Set working directory
project_root = Path(__file__).resolve().parents[1]
os.chdir(project_root)
print(f"📁 Working in: {os.getcwd()}")

# File paths
input_pdf_path = os.path.join("reports", "Hospital Cost Reports.pdf")
output_folder = os.path.join("data")
os.makedirs(output_folder, exist_ok=True)

# Page mapping (0-indexed)
schedule_vii_map = {
    "schedule_vii": [50, 51],                 # Pages 51–52
    "schedule_viib": [55],                    # Page 56
    "schedule_viic": [56],                    # Page 57
    "schedule_viid": [57],                    # Page 58
}
schedule_viia_pages = {
    "viia_part1": 52,  # Page 53
    "viia_part2": 53,  # Page 54
    "viia_part3": 54   # Page 55
}

# Detect header lines
def detect_column_headers(page):
    words = page.extract_words()
    header_lines = defaultdict(list)
    for word in words:
        if word["top"] < 150:
            y = round(word["top"], 1)
            header_lines[y].append((word["x0"], word["text"]))
    sorted_lines = []
    for y in sorted(header_lines.keys()):
        line = " ".join(text for _, text in sorted(header_lines[y]))
        sorted_lines.append(line)
    return sorted_lines

# Extract rows
def extract_rows(page, key_column="Description"):
    words = page.extract_words(use_text_flow=True)
    lines_by_y = defaultdict(list)
    for word in words:
        y_key = round(word["top"], 1)
        lines_by_y[y_key].append((word["x0"], word["text"]))

    rows = []
    for y, line_words in sorted(lines_by_y.items()):
        sorted_line = [text for x, text in sorted(line_words)]
        if not sorted_line:
            continue
        if sorted_line[0].replace(".", "", 1).isdigit():
            line_no = sorted_line[0]
            desc_words = []
            rest = []
            for token in sorted_line[1:]:
                if re.match(r"^\d[\d,]*\.?\d*$", token):
                    rest.append(token)
                elif not rest:
                    desc_words.append(token)
                else:
                    rest.append(token)
            description = " ".join(desc_words)
            row = [line_no, description] + rest
            rows.append(row)
    return rows

# 📥 Process Schedule VII + VII-B, VII-C, VII-D
with pdfplumber.open(input_pdf_path) as pdf:
    for label, pages in schedule_vii_map.items():
        rows = []
        headers = []
        for page_num in pages:
            page = pdf.pages[page_num]
            header_lines = detect_column_headers(page)
            if header_lines:
                col_line = max(header_lines, key=lambda l: len(l.split()))
                headers = ["Line No.", "Description"] + col_line.split()[2:]
            rows.extend(extract_rows(page))

        if rows:
            max_len = max(len(row) for row in rows)
            while len(headers) < max_len:
                headers.append(f"{label}_col{len(headers)+1}")
            df = pd.DataFrame(rows, columns=headers[:max_len])
            df.to_csv(os.path.join(output_folder, f"{label}.csv"), index=False)
            print(f"✅ Saved {label}.csv")
        else:
            print(f"⚠️ No data extracted for {label}")

    # 📦 Process Schedule VII-A (pages 53–55) — merge by Description
    vii_a_parts = []
    for part_name, page_index in schedule_viia_pages.items():
        page = pdf.pages[page_index]
        header_lines = detect_column_headers(page)
        if header_lines:
            col_line = max(header_lines, key=lambda l: len(l.split()))
            headers = ["Line No.", "Description"] + col_line.split()[2:]
        else:
            headers = ["Line No.", "Description"]

        rows = extract_rows(page)
        if rows:
            max_len = max(len(row) for row in rows)
            while len(headers) < max_len:
                headers.append(f"{part_name}_col{len(headers)+1}")
            df = pd.DataFrame(rows, columns=headers[:max_len])
            df = df.drop(columns=["Line No."], errors='ignore')
            df = df.drop_duplicates(subset=["Description"])
            df.set_index("Description", inplace=True)
            vii_a_parts.append(df)
            print(f"✅ Extracted {part_name} with {df.shape[1]} columns")
        else:
            print(f"⚠️ No data in {part_name}")

    # 🔗 Merge VII-A parts side by side on Description
    if vii_a_parts:
        merged_viia = pd.concat(vii_a_parts, axis=1)
        merged_viia.reset_index(inplace=True)
        merged_viia.to_csv(os.path.join(output_folder, "schedule_viia_merged.csv"), index=False)
        print("✅ Merged and saved: schedule_viia_merged.csv")
    else:
        print("❌ No VII-A data to merge")
