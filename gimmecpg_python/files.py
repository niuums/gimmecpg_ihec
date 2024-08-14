"""Input and output files."""

import concurrent.futures
from pathlib import Path

import polars as pl


def collapse_strands(bed):
    """Collapse strands."""
    pos = bed.filter(pl.col("strand") == "+")
    neg = bed.filter(pl.col("strand") == "-").with_columns(
        (pl.col("start")).alias("cStart")
    )  # add column for start site on complementary strand

    joint = pos.join(neg, left_on=["chr", "end"], right_on=["chr", "cStart"], how="full", coalesce=True).with_columns(
        pl.concat_str([pl.col("strand"), pl.col("strand_right")], separator="/", ignore_nulls=True)
    )

    merged = (
        joint.with_columns(
            pl.min_horizontal("start", "start_right").alias("start"),
            pl.max_horizontal("end", "end_right").alias("end"),
            pl.col(["percent_methylated_right", "coverage_right", "percent_methylated", "coverage"])
            .fill_null(0)
            .cast(pl.UInt64),
        )
        .with_columns((pl.col("coverage") + pl.col("coverage_right")).alias("total_coverage"))
        .filter(pl.col("total_coverage") > 0)
        .with_columns(
            (
                (
                    pl.col("coverage") * pl.col("percent_methylated")
                    + pl.col("coverage_right") * pl.col("percent_methylated_right")
                )
                / pl.col("total_coverage")
            ).alias("avg")
        )  # calculated weighted average
    )

    return merged


def read_files(file, mincov, collapse):
    """Scan files."""
    print(f"not using 'mincov = {mincov} and collapse = {collapse}' options")
    name = Path(file).stem
    print(f"Scanning {name}")
    bed = (
        pl.scan_csv(
            file,
            separator=",", # convert to "," for actual files
            has_header=True
        )  # cannot use scan() on zipped file
    )

    bed_df = bed.melt(id_vars="l").collect().partition_by("variable", maintain_order = True)
    bed_lf = [pl.LazyFrame(df) for df in bed_df]

    return bed_lf


def save_files(df, outpath):
    """Save files w/o streaming."""
    filename = (
        df.unique(subset="sample", keep="any")
        .select(pl.col("sample").filter(pl.col("sample") != "imputed").first())
        .item()
    )
    outfile = Path(outpath, "imputed_" + filename + ".bed")
    print(f"Saving {filename}")
    df.write_csv(outfile, separator="\t")
    return f"Saved {filename}"


def parallel_save(dfs, outpath):
    """Save files in parallel."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(save_files, df, outpath): df for df in dfs}

    for future in concurrent.futures.as_completed(futures):
        df = futures[future]
        result = future.result()
        print(f"{result}")
    return df


# def save_files_normal(file, outpath):
#     """Save files w/o streaming."""
#     filename = (
#         file.unique(subset="sample", keep="any")
#         .select(pl.col("sample").filter(pl.col("sample") != "imputed").first())
#         .item()
#     )
#     outfile = Path(outpath, "imputed_" + filename + ".bed")
#     print(f"Saving {filename}")
#     file.write_csv(outfile, separator="\t")
#     return f"Saved {filename}"


# def save_files_streaming(file, outpath):
#     """Save files by streaming."""
#     # file = file.collect(streaming=True)
#     filename = (
#         file.unique(subset="sample", keep="any")
#         .select(pl.col("sample").filter(pl.col("sample") != "imputed").first())
#         .item()
#     )
#     outfile = Path(outpath, "imputed_" + filename + ".bed")
#     print(f"Saving {filename}")
#     file.write_csv(outfile, separator="\t")
#     return f"Saved {filename}"
