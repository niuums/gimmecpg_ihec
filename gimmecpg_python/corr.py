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
        ).melt(id_vars="l")
    ).cast({"l": pl.Int32, "value": pl.Float32})

    print("Scanned ChromImpute file. Scanning original file now")

    original = (
        pl.scan_csv(
            f"/home/nchai/scratch/ihec/original/chr{x}.meth10.csv",
            separator=",", # convert to "," for actual files
            has_header=True,
            low_memory = True
        ).melt(id_vars="l")
    ).cast({"l": pl.Int32, "value": pl.Float32})


    print("Both files scanned")

    original = original.filter(pl.col("value") != -1)
    # chromimpute = chromimpute.filter(pl.col("value").is_null())

    # print(chromimpute.fetch(10000))
    # exit()
    compare = original.join(chromimpute, on = ["l", "variable"], how = "inner", suffix = "_chromimpute")

    corr = compare.group_by("variable").agg(pl.corr("value", "value_chromimpute", method = "pearson"))
    
    print("Collecting and saving")

    corr.collect(streaming = True).write_csv(f"/home/nchai/scratch/ihec/corr_chrImp/chr{x}_corr.csv")

    print(f"Done for Chromosome {x}")
