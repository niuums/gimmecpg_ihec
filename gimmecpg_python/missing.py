"""Identify missing sites."""

import polars as pl


def missing_sites(bed):
    """Compare to reference."""
    missing = (
        bed.with_columns(value = pl.col("value").replace(-1.0, None))
        .with_columns(
            pl.when(pl.col("value").is_not_null()).then(pl.col("l")).alias("b_start"),
            pl.when(pl.col("value").is_not_null()).then(pl.col("l")).alias("f_start"),
            pl.when(pl.col("value").is_not_null()).then(pl.col("value")).alias("b_meth"),
            pl.when(pl.col("value").is_not_null()).then(pl.col("value")).alias("f_meth"),
        )
        .with_columns(
            pl.col(["f_start", "f_meth"]).backward_fill(),
            pl.col(["b_start", "b_meth"]).forward_fill()
        )
        .with_columns(
            (pl.col("l") - pl.col("b_start")).alias("b_dist"), (pl.col("f_start") - pl.col("l")).alias("f_dist")
        )
    )

    return missing
