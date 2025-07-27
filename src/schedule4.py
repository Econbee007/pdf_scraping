import pdfplumber
import pandas as pd
import os
import re
from pathlib import Path
from collections import defaultdict

# Setup
project_root =  Path(__file__).resolve().parents[1]
os.chdir(project_root)
print(f"Working in: {os.getcwd()}")

# Paths
input_pdf_path = os.path.join("reports", "Hospital Cost Reports.pdf")
output_folder = os.path.join("data")
os.makedirs(output_folder, exist_ok=True)

# 🗂 Define page ranges per division (0-based page index)
schedule_iv_pages = {
    "IV-A": [20, 21],     # pages 21–22
    "IV-B": [22],         # page 23
    "IV-C": [23],         # page 24
    "IV-D": [23],         # page 24 (shared)
    "IV-E": [24],         # page 25
}

# Detect headers at top of page
def detect_column_headers(page):
    words = page.extract_words()
    header_lines = defaultdict(list)

    for word in words:
        if word["top"] < 150:  # near top of page
            y = round(word["top"], 1)
            header_lines[y].append((word["x0"], word["text"]))

    sorted_lines = []
    for y in sorted(header_lines.keys()):
        line = " ".join(text for _, text in sorted(header_lines[y]))
        sorted_lines.append(line)

    return sorted_lines

# Extract rows from body
def extract_rows(page, headers):
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

        # Detect data row (starts with line number or number)
        if sorted_line[0].replace(".", "", 1).isdigit():
            line_no = sorted_line[0]
            description_words = []
            rest_values = []

            for token in sorted_line[1:]:
                if re.match(r"^\d[\d,]*\.?\d*$", token):
                    rest_values.append(token)
                elif not rest_values:
                    description_words.append(token)
                else:
                    rest_values.append(token)

            description = " ".join(description_words)
            row = [line_no, description] + rest_values
            rows.append(row)
    return rows

# Process each division
with pdfplumber.open(input_pdf_path) as pdf:
    for division, page_indices in schedule_iv_pages.items():
        extracted_rows = []
        headers = []

        for i in page_indices:
            page = pdf.pages[i]
            header_lines = detect_column_headers(page)

            # Pick the longest top line as headers
            if header_lines:
                headers = max(header_lines, key=lambda l: len(l.split()))
                headers = ["Line No.", "Cost Center Description"] + headers.split()[2:]

            rows = extract_rows(page, headers)
            extracted_rows.extend(rows)

        # Save to CSV
        if extracted_rows and headers:
            max_len = max(len(row) for row in extracted_rows)
            while len(headers) < max_len:
                headers.append(f"Column_{len(headers)+1}")

            df = pd.DataFrame(extracted_rows, columns=headers[:max_len])
            csv_path = os.path.join(output_folder, f"schedule_iv_{division.lower()}.csv")
            df.to_csv(csv_path, index=False)
            print(f"✅ Schedule IV - {division} extracted and saved to: {csv_path}")
        else:
            print(f"⚠️ No data extracted for Schedule IV - {division}")
