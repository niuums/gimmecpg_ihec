import polars as pl

# chr = [15, 16, 17, 18, 19, 20, 21, 22, "X"] # 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 

# for x in chr:
#     print(f"Calculating for Chromosome {x}")

#     chromimpute = (
#         pl.scan_csv(
#             f"/home/nchai/scratch/ihec/coverages/chr{x}.cov.csv",
#             separator=",", 
#             has_header=True,
#             low_memory = True
#         ).unpivot(index="l")
#     )

#     print("Scanned ChromImpute file")

#     avg = chromimpute.group_by("variable").agg(pl.mean("value"))

#     print("Collecting and saving")

#     avg.collect().write_csv(f"/home/nchai/scratch/ihec/coverage_per_chr/chr{x}_avgCov.csv")

#     print(f"Done for Chromosome {x}")


#################################
# Run after first step finished #
#################################

files = pl.scan_csv("/home/nchai/scratch/ihec/coverage_per_chr/*.csv")

avg = files.group_by("variable").agg(pl.mean("value"))

avg.collect().write_csv("/home/nchai/scratch/ihec/coverages.csv")