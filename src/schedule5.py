import pdfplumber
import pandas as pd
import os
import re
from pathlib import Path
from collections import defaultdict

# ✅ Set working directory to project root
project_root =  Path(__file__).resolve().parents[1]
os.chdir(project_root)
print(f"📁 Working in: {os.getcwd()}")

# 📄 File path
input_pdf_path = os.path.join("reports", "Hospital Cost Reports.pdf")
output_folder = os.path.join("data")
os.makedirs(output_folder, exist_ok=True)

# 📊 Define schedule page ranges (0-based)
schedule_v_pages = {
    "schedule_va_part1": list(range(25, 28)),   # Pages 26–28
    "schedule_va_part2": list(range(28, 31)),   # Pages 29–31
    "schedule_va_part3": list(range(31, 34)),   # Pages 32–34
    "schedule_vb": [34],                        # Page 35
}

# 🔍 Detect column headers at top
def detect_column_headers(page):
    words = page.extract_words()
    header_lines = defaultdict(list)

    for word in words:
        if word["top"] < 150:  # near top
            y = round(word["top"], 1)
            header_lines[y].append((word["x0"], word["text"]))

    sorted_lines = []
    for y in sorted(header_lines.keys()):
        line = " ".join(text for _, text in sorted(header_lines[y]))
        sorted_lines.append(line)

    return sorted_lines

# 🧾 Extract rows under headers
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

        # Row starts with a line number or code
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

# 🔁 Extract each part
with pdfplumber.open(input_pdf_path) as pdf:
    for part_name, pages in schedule_v_pages.items():
        extracted_rows = []
        headers = []

        for i in pages:
            page = pdf.pages[i]
            header_lines = detect_column_headers(page)

            if header_lines:
                # Use longest header line
                headers = max(header_lines, key=lambda l: len(l.split()))
                headers = ["Line No.", "Cost Center Description"] + headers.split()[2:]

            rows = extract_rows(page, headers)
            extracted_rows.extend(rows)

        # Save CSV
        if extracted_rows and headers:
            max_len = max(len(row) for row in extracted_rows)
            while len(headers) < max_len:
                headers.append(f"Column_{len(headers)+1}")

            df = pd.DataFrame(extracted_rows, columns=headers[:max_len])
            csv_path = os.path.join(output_folder, f"{part_name}.csv")
            df.to_csv(csv_path, index=False)
            print(f"✅ Extracted and saved: {csv_path}")
        else:
            print(f"⚠️ No data extracted for: {part_name}")
