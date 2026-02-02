import os
from shutil import rmtree

import pandas as pd
from openpyxl import load_workbook


def replace_fields(
    dict_path: str = "config/dict_zh.xlsx",
    sheet_name: str = "dict",
    orig_folder: str = "mid_files/tmp_find_header_row",
    result_folder: str = "mid_files/tmp_field_replace",
    clear_output: bool = False,
) -> None:
    dict_df = pd.read_excel(dict_path, sheet_name=sheet_name)
    dict_df = dict_df.sort_values(by=["new_field", "priority"], ascending=True)

    if not os.path.exists(result_folder):
        os.mkdir(result_folder)
    elif clear_output:
        rmtree(result_folder)
        os.mkdir(result_folder)

    new_fields = dict_df["new_field"].unique()

    for filename in os.listdir(orig_folder):
        new_filename = f"{filename.rsplit('.', 1)[0]}.{filename.rsplit('.', 1)[1]}"

        if filename.endswith(".csv"):
            full_path = os.path.join(orig_folder, filename)
            if os.path.getsize(full_path) == 0:
                print(f"Ignored empty file {filename}")
                continue
            df = pd.read_csv(full_path)
            for new_field in new_fields:
                for _, row in dict_df[dict_df["new_field"] == new_field].iterrows():
                    if row["old_field"] in df.columns:
                        df.rename(
                            columns={row["old_field"]: row["new_field"]}, inplace=True
                        )
                        break
            df.to_csv(os.path.join(result_folder, new_filename), index=False)

        elif filename.endswith(".xlsx"):
            book = load_workbook(os.path.join(orig_folder, filename))
            writer = pd.ExcelWriter(
                os.path.join(result_folder, new_filename), engine="openpyxl"
            )
            writer.book = book
            for sheet in book.sheetnames:
                if book[sheet].calculate_dimension() == "A1":
                    print(f"Ignored empty sheet {sheet} in {filename}")
                    continue
                df = pd.read_excel(
                    os.path.join(orig_folder, filename), sheet_name=sheet
                )
                for new_field in new_fields:
                    for _, row in dict_df[dict_df["new_field"] == new_field].iterrows():
                        if row["old_field"] in df.columns:
                            df.rename(
                                columns={row["old_field"]: row["new_field"]},
                                inplace=True,
                            )
                            break
                df.to_excel(writer, index=False, sheet_name=sheet)
            writer.save()
            writer.close()
        else:
            print(f"Ignored file {filename} as it is not a csv or xlsx file.")
