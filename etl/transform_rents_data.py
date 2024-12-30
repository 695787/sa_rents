import os
import pandas as pd
import duckdb

root_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sa_rents_data_folder = os.path.join(root_folder, "sa_rents_data")
output_file = os.path.join(root_folder, "clean_data", "sa_rents_data.csv")
ddb_file = os.path.join(root_folder, "clean_data", "sa_rents_data.duckdb")

con = duckdb.connect(ddb_file)


all_data = []


# format of the columns in the data files changed over time

cols_202012_to_202409 = [
    "data_quarter",
    "year",
    "month",
    "suburb",
    "br1_flat_count",
    "br1_flat_median_price",
    "br2_flat_count",
    "br2_flat_median_price",
    "br3_flat_count",
    "br3_flat_median_price",
    "br4_flat_count",
    "br4_flat_median_price",
    "unknown_br_flat_count",
    "unknown_br_flat_median_price",
    "total_flat_count",
    "total_flat_median_price",
    "br1_house_count",
    "br1_house_median_price",
    "br2_house_count",
    "br2_house_median_price",
    "br3_house_count",
    "br3_house_median_price",
    "br4_house_count",
    "br4_house_median_price",
    "unknown_br_house_count",
    "unknown_br_house_median_price",
    "total_house_count",
    "total_house_median_price",
    "other_count",
    "other_median_price",
    "other_count_dup",
    "other_median_price_dup",
    "total_count",
    "total_median_price",
]

cols_202409 = [
    "data_quarter",
    "year",
    "month",
    "suburb",
    "br1_flat_count",
    "br1_flat_median_price",
    "br2_flat_count",
    "br2_flat_median_price",
    "br3_flat_count",
    "br3_flat_median_price",
    "br4_flat_count",
    "br4_flat_median_price",
    "total_flat_count",
    "total_flat_median_price",
    "br1_house_count",
    "br1_house_median_price",
    "br2_house_count",
    "br2_house_median_price",
    "br3_house_count",
    "br3_house_median_price",
    "br4_house_count",
    "br4_house_median_price",
    "total_house_count",
    "total_house_median_price",
    "other_count",
    "other_median_price",
    "other_count_dup",
    "other_median_price_dup",
    "total_count",
    "total_median_price",
]


cols_200806_to_202009 = [
    "data_quarter",
    "year",
    "month",
    "metro/ros",
    "suburb",
    "br1_flat_count",
    "br1_flat_median_price",
    "br2_flat_count",
    "br2_flat_median_price",
    "br3_flat_count",
    "br3_flat_median_price",
    "br4_flat_count",
    "br4_flat_median_price",
    "unknown_br_flat_count",
    "unknown_br_flat_median_price",
    "total_flat_count",
    "total_flat_median_price",
    "br1_house_count",
    "br1_house_median_price",
    "br2_house_count",
    "br2_house_median_price",
    "br3_house_count",
    "br3_house_median_price",
    "br4_house_count",
    "br4_house_median_price",
    "unknown_br_house_count",
    "unknown_br_house_median_price",
    "total_house_count",
    "total_house_median_price",
    "other_count",
    "other_median_price",
    "total_count",
    "total_median_price",
]


for file_name in os.listdir(sa_rents_data_folder):
    if file_name.endswith(".xlsx") or file_name.endswith(".xls"):
        date_part = file_name.split("_")[-1].split(".")[0]
        year, month = date_part.split("-")
        date_columns = pd.DataFrame(
            [[date_part, year, month]], columns=["Date", "Year", "Month"]
        )
        file_path = os.path.join(sa_rents_data_folder, file_name)
        try:
            try:
                df = pd.read_excel(file_path, sheet_name="Suburb")
            except ValueError:
                try:
                    df = pd.read_excel(file_path, sheet_name="Suburbs")
                except ValueError:
                    df = pd.read_excel(file_path, sheet_name="Final Suburbs")

            # This is hacky, but the data format is not consistent!!
            # Check for exact match for "Metro" in the first column
            metro_row = df[df.iloc[:, 0] == "Metro"]
            if not metro_row.empty:
                # Extract data from Metro row onwards
                df = df.loc[metro_row.index[0] :].reset_index(drop=True)
                df = pd.concat([date_columns] * len(df), ignore_index=True).join(
                    df.reset_index(drop=True)
                )

                # Determine the appropriate columns based on the date
                if "2008-06" <= date_part <= "2020-09":
                    df.columns = cols_200806_to_202009
                elif "2020-12" <= date_part < "2024-09":
                    df.columns = cols_202012_to_202409
                elif date_part == "2024-09":
                    df.columns = cols_202409
                else:
                    print(
                        f"Date {date_part} in {file_path} is out of the expected range."
                    )
                    continue
                all_data.append(df)
            else:
                print(f"No metro row found in {file_path}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    con.execute("CREATE TABLE IF NOT EXISTS sa_rents_data AS SELECT * FROM combined_df")
    combined_df.to_csv(output_file, index=False)
    print(f"Combined data saved to {output_file}")
else:
    print("No data found to combine.")


query = """
SELECT data_quarter, year, month, suburb, br1_flat_count, br1_flat_median_price
FROM sa_rents_data
WHERE suburb = 'Adelaide' AND cast(year AS INTEGER)  BETWEEN 2008 AND 2024
ORDER BY year, month
"""

adelaide_br1_flat_prices = con.execute(query).fetchdf()
print(adelaide_br1_flat_prices)
