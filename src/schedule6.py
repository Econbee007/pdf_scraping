import pdfplumber
import pandas as pd
import os
import re
from pathlib import Path
from collections import defaultdict

# 📁 Set working directory
project_root = Path(__file__).resolve().parents[1]
os.chdir(project_root)
print(f"📁 Working in: {os.getcwd()}")

# 📄 Paths
input_pdf_path = os.path.join("reports", "Hospital Cost Reports.pdf")
output_folder = os.path.join("data")
os.makedirs(output_folder, exist_ok=True)
output_csv_path_vi = os.path.join(output_folder, "schedule_vi_merged.csv")
output_csv_path_via = os.path.join(output_folder, "schedule_via.csv")

# 📊 Schedule VI parts (0-indexed)
schedule_vi_parts = {
    "part1": [35, 36],  # Pages 36–37
    "part2": [37, 38],  # Pages 38–39
    "part3": [39, 40],  # Pages 40–41
    "part4": [41, 42],  # Pages 42–43
    "part5": [43, 44],  # Pages 44–45
    "part6": [45, 46],  # Pages 46–47
    "part7": [47, 48],  # Pages 48–49
}
schedule_via_page_index = 49  # Page 50 (0-indexed)

# 🔍 Detect column headers
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

# 🧾 Extract rows from page
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

# 📦 Store VI part DataFrames
dataframes = []

# 🔁 Process VI parts
with pdfplumber.open(input_pdf_path) as pdf:
    for part_label, pages in schedule_vi_parts.items():
        extracted_rows = []
        headers = []

        for i in pages:
            page = pdf.pages[i]
            header_lines = detect_column_headers(page)
            if header_lines:
                col_line = max(header_lines, key=lambda l: len(l.split()))
                headers = ["Line No.", "Cost Center Description"] + col_line.split()[2:]
            rows = extract_rows(page, headers)
            extracted_rows.extend(rows)

        if extracted_rows and headers:
            max_len = max(len(r) for r in extracted_rows)
            while len(headers) < max_len:
                headers.append(f"{part_label}_col{len(headers)+1}")
            df = pd.DataFrame(extracted_rows, columns=headers[:max_len])
            df = df.drop_duplicates(subset=["Line No.", "Cost Center Description"])
            df = df.set_index(["Line No.", "Cost Center Description"])
            dataframes.append(df)
            print(f"✅ Extracted {part_label} with {df.shape[1]} columns.")
        else:
            print(f"⚠️ No data extracted for {part_label}")

    # 🔗 Merge all Schedule VI parts
    if dataframes:
        merged_df = pd.concat(dataframes, axis=1)
        merged_df.reset_index(inplace=True)
        merged_df.to_csv(output_csv_path_vi, index=False)
        print(f"\n✅ Schedule VI merged and saved to: {output_csv_path_vi}")
    else:
        print("❌ No Schedule VI data extracted.")

    # 🧾 Now extract Schedule VI-A
    via_page = pdf.pages[schedule_via_page_index]
    via_headers = []
    header_lines = detect_column_headers(via_page)
    if header_lines:
        col_line = max(header_lines, key=lambda l: len(l.split()))
        via_headers = ["Line No.", "Cost Center Description"] + col_line.split()[2:]
    via_rows = extract_rows(via_page, via_headers)

    if via_rows and via_headers:
        max_len = max(len(row) for row in via_rows)
        while len(via_headers) < max_len:
            via_headers.append(f"col{len(via_headers)+1}")
        df_via = pd.DataFrame(via_rows, columns=via_headers[:max_len])
        df_via.to_csv(output_csv_path_via, index=False)
        print(f"✅ Schedule VI-A extracted and saved to: {output_csv_path_via}")
    else:
        print("⚠️ No data found on Schedule VI-A.")
