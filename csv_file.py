import csv

# with open("carrefour-ksa-items.csv", "r", newline="") as f_input:
#     csv_input = csv.DictReader(f_input)
#     data = sorted(csv_input, key=lambda row: (row["catalog_uuid"]))

#     with open("output_1.csv", "w", newline="") as f_output:
#         csv_output = csv.DictWriter(f_output, fieldnames=csv_input.fieldnames)
#         csv_output.writeheader()
#         for row in data:
#             csv_output.writerow((data[0], data[1], data[2]))

with open("carrefour-ksa-items.csv", "r") as source:
    raw_reader = csv.reader(source)
    header = next(raw_reader, None)
    sorted_data = sorted(raw_reader, key=lambda row: (row[1]))
    with open("output_1.csv", "w") as result:
        writer = csv.writer(result)
        writer.writerow(header)
        for r in sorted_data:
            writer.writerow(
                (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[11])
            )
