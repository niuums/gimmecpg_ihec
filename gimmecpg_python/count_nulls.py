import polars as pl
from pathlib import Path


chr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, "X", "Y"]

files = [] # chromosome
# total_null_count = [] # total nulls per chromosome, across all samples
# sites_with_nulls = [] # CpG sites (ie. rows) per chromosome with at least 1 null, across all samples
sites_some_nulls = [] # CpG sites (ie. rows) per chromosome with at least 322 (1/2 of 645) nulls, across all samples
# total_sites = [] # total number of sites per chromosome

for x in chr:
    print(f"Processing Chromosome {x}")

    bed = (
    pl.scan_csv(
            f"/home/nchai/scratch/ihec/original/chr{x}.meth10.csv",
            separator=",", # convert to "," for actual files
            has_header=True,
            null_values = "-1.0"
        )
    )

    count_mat = bed.with_columns(pl.sum_horizontal(pl.all(ignore_nulls = False).is_null()).alias("null_counts"))

    tally = count_mat.select(["l", "null_counts"]).collect()

    # total_nulls = tally.select("null_counts").sum()
    # with_nulls = tally.filter(pl.col("null_counts") > 0).select(pl.count("l"))
    some_nulls = tally.filter(pl.col("null_counts") > 580).select(pl.count("l"))
    # all_sites = tally.select(pl.count("l"))

    files.append(f"imputed_chr{x}_meth10")
    # total_null_count.append(total_nulls.item())
    # sites_with_nulls.append(with_nulls.item())
    sites_some_nulls.append(some_nulls.item())
    # total_sites.append(all_sites.item())



# df = pl.DataFrame({"chromosome": files, 
#         "total_null_count": total_null_count, 
#         "sites_with_nulls" : sites_with_nulls, 
#         "sites_half_nulls": sites_half_nulls,
#         "total_sites" : total_sites})

        
df = pl.DataFrame({"chromosome": files, 
        "sites_some_nulls": sites_some_nulls})

print(df)
df.write_csv("/home/nchai/scratch/ihec/90_summary.csv")




## Count number of sites that got imputed ##

# files = [] # chromosome
# sites_imputed = []
# total_sites = [] # total number of sites per chromosome

# for x in chr:
#     print(f"Processing imputed Chromosome {x}")

#     bed = (
#     pl.scan_csv(
#             f"/home/nchai/scratch/ihec/gimmecpg_imputed/gimmecpg_chr{x}.meth10.bed",
#             separator="\t", # convert to "," for actual files
#             has_header=True
#         )
#     )

#     count_mat = (
#         bed.with_columns(pl.sum_horizontal(pl.all(ignore_nulls = False).is_not_null()).alias("imputed_counts")
#         )
#         .with_columns((pl.col("imputed_counts") - 1).alias("imputed_counts")
#         )
#     )

#     tally = count_mat.select(["l", "imputed_counts"]).collect()

#     imputed = tally.select("imputed_counts").sum()
#     all_sites = tally.select(pl.count("l"))

#     # print(count_mat.fetch(100))
#     # exit()

#     files.append(f"imputed_chr{x}_meth10")
#     sites_imputed.append(imputed.item())
#     total_sites.append(all_sites.item())



# df = pl.DataFrame({"chromosome": files, 
#         "sites_imputed": sites_imputed, 
#         "total_sites" : total_sites})

# print(df)
# df.write_csv("/home/nchai/scratch/ihec/gimmecpg_imputed_summary.csv")