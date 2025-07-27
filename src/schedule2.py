import pdfplumber
import pandas as pd
import os
import re
from pathlib import Path
from collections import defaultdict

# Set working directory
project_root = Path.cwd()
os.chdir(project_root)
print(f"📁 Working in: {os.getcwd()}")

# File paths
input_pdf_path = os.path.join("reports", "Hospital Cost Reports.pdf")
output_csv_path = os.path.join("data", "schedule_ii_merged.csv")
os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

# Define column groups by page ranges
column_groups = {
    (3, 6): [
        "Line No.", "Cost Center Description",
        "Expense Before Reclassi-fication", "Direct Expense",
        "Expense After Stepdown(Excl Cap)", "Expense After Stepdown(Incl Cap)",
        "Patient Service Expense By Dept(Excl Cap)", "Patient Service Expense By Dept(Incl Cap)"
    ],
    (7, 10): [
        "Line No.", "Cost Center Description",
        "Gross Revenue by Department", "Patient Expense by Service(Excl Cap)",
        "Patient Expense by Service(Incl Cap)", "Gross Revenue by Service",
        "Non-Physician FTE", "Physician FTE"
    ],
    (11, 14): [
        "Line No.", "Cost Center Description",
        "Number of Units", "Unit of measure"
    ]
}

# Master dict to collect all merged values by (line no, cost center)
merged_data = defaultdict(dict)

def extract_rows_from_page(page, headers, page_num):
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

        # Must start with a line number
        if sorted_line[0].replace(".", "", 1).isdigit():
            line_no = sorted_line[0]

            # Parse description and values
            description_words = []
            rest_values = []
            for token in sorted_line[1:]:
                if re.match(r"^\d[\d,]*\.?\d*$", token):  # looks like a number
                    rest_values.append(token)
                elif not rest_values:
                    description_words.append(token)
                else:
                    rest_values.append(token)

            cost_center = " ".join(description_words)
            row = [page_num, line_no, cost_center] + rest_values
            rows.append(row)
    return rows

# Process each page group
for (start_page, end_page), headers in column_groups.items():
    for i in range(start_page, end_page + 1):
        with pdfplumber.open(input_pdf_path) as pdf:
            page = pdf.pages[i]
            page_num = i + 1
            rows = extract_rows_from_page(page, headers, page_num)

            for row in rows:
                line_no = row[1].strip()
                cost_center = row[2].strip()
                values = row[3:]

                key = (line_no, cost_center)
                data_dict = dict(zip(headers[2:], values))  # skip line no + description
                merged_data[key].update(data_dict)

# Define full final column structure
full_columns = [
    "Line No.", "Cost Center Description",
    "Expense Before Reclassi-fication", "Direct Expense",
    "Expense After Stepdown(Excl Cap)", "Expense After Stepdown(Incl Cap)",
    "Patient Service Expense By Dept(Excl Cap)", "Patient Service Expense By Dept(Incl Cap)",
    "Gross Revenue by Department", "Patient Expense by Service(Excl Cap)",
    "Patient Expense by Service(Incl Cap)", "Gross Revenue by Service",
    "Non-Physician FTE", "Physician FTE",
    "Number of Units", "Unit of measure"
]

# Assemble final data
all_keys = sorted(merged_data.keys(), key=lambda x: float(x[0]) if x[0].replace(".", "", 1).isdigit() else x[0])
final_data = []
for key in all_keys:
    line_no, cost_center = key
    row_dict = {"Line No.": line_no, "Cost Center Description": cost_center}
    row_dict.update(merged_data[key])
    final_data.append([row_dict.get(col, "") for col in full_columns])

# Export to CSV
df = pd.DataFrame(final_data, columns=full_columns)
df.to_csv(output_csv_path, index=False)

print(f"\n✅ Schedule II fully extracted and aligned. Saved to: {output_csv_path}")
