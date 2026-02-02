import pandas as pd


def array_agg_optimized(
    input_file: str, dimension_column: str, array_column: str, output_file: str
) -> None:
    data_df = pd.read_csv(input_file)
    if dimension_column not in data_df.columns or array_column not in data_df.columns:
        raise ValueError("Specified columns are not found in the input file.")

    result = (
        data_df.groupby(dimension_column)[array_column]
        .agg(
            [
                ("数组", lambda x: "{" + ",".join(x.unique().astype(str)) + "}"),
                ("个数", "nunique"),
            ]
        )
        .reset_index()
    )

    result.columns = [dimension_column, f"{array_column}数组", f"{array_column}个数"]
    result.to_csv(output_file, index=False)
