import pdfplumber
import pandas as pd
import os
import re
from pathlib import Path
from collections import defaultdict

# 📁 Set project root and change working directory
project_root = Path.cwd()  # or use Path("D:/RA_tamanna/scrapping") if needed
os.chdir(project_root)
print(f"📁 Working in: {os.getcwd()}")

# 📄 PDF and output paths
input_pdf_path = os.path.join("reports", "Hospital Cost Reports.pdf")
output_folder = os.path.join("data")
os.makedirs(output_folder, exist_ok=True)

# 📊 Define schedules and their page ranges (0-based index)
schedule_pages = {
    "Schedule III": list(range(15, 18)),   # pages 16–18
    "Schedule IIIA": [18],                 # page 19
    "Schedule IIIB": [19]                  # page 20
}

# 🔍 Try to detect column headers at top of page
def detect_column_headers(page):
    words = page.extract_words()
    header_lines = defaultdict(list)

    for word in words:
        if word["top"] < 150:  # top section of page
            y = round(word["top"], 1)
            header_lines[y].append((word["x0"], word["text"]))

    sorted_lines = []
    for y in sorted(header_lines.keys()):
        line = " ".join(text for _, text in sorted(header_lines[y]))
        sorted_lines.append(line)

    return sorted_lines

# 🧾 Extract data rows using position and numeric heuristics
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

        # Row starts with numeric line number
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

# 🔁 Process each schedule
for schedule_name, page_indices in schedule_pages.items():
    extracted_rows = []
    headers = []

    with pdfplumber.open(input_pdf_path) as pdf:
        for i in page_indices:
            page = pdf.pages[i]
            page_num = i + 1
            header_lines = detect_column_headers(page)

            # Use the longest line near top as header
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
        csv_path = os.path.join(output_folder, f"{schedule_name.lower().replace(' ', '_')}.csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ {schedule_name} extracted and saved to: {csv_path}")
    else:
        print(f"⚠️ No data extracted for {schedule_name}.")
