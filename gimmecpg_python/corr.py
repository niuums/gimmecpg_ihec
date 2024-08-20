import polars as pl

chr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, "X"]

for x in chr:
    print(f"Calculating for Chromosome {x}")

    chromimpute = (
        pl.scan_csv(
            f"/home/nchai/scratch/ihec/imputed/chr{x}.imputed.csv",
            separator=",", 
            has_header=True,
            low_memory = True
        ).unpivot(index="l")
    ).cast({"l": pl.Int32, "value": pl.Float32})

    print("Scanned ChromImpute file. Scanning GIMMEcpg file now")

    gimmecpg = (
        pl.scan_csv(
            f"/home/nchai/scratch/ihec/gimmecpg_imputed_meth3/gimmecpg_chr{x}.meth3.bed",
            separator="\t", # convert to "," for actual files
            has_header=True,
            low_memory = True
        ).unpivot(index="l")
    ).cast({"l": pl.Int32, "value": pl.Float32})

    print("Both files scanned")

    gimmecpg2 = gimmecpg.drop_nulls()
    chromimpute2 = chromimpute.drop_nulls()

    compare = gimmecpg2.join(chromimpute2, on = ["l", "variable"], how = "inner", suffix = "_chromimpute")

    corr = compare.group_by("variable").agg(pl.corr("value", "value_chromimpute", method = "pearson"))
    
    print("Collecting and saving")

    corr.collect().write_csv(f"/home/nchai/scratch/ihec/corr3/chr{x}_corr.csv")

    print(f"Done for Chromosome {x}")
