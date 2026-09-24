# Benchmark a GFA 1.0 graph with unsuffixed PanSN P path names.
configfile: "config.p.yaml"

PATH_FORMAT = "P"

include: "rules/common.smk"


# Define the final target in the entry point so Snakemake selects it when the
# workflow is invoked without an explicit rule name.
localrules: all


rule all:
    default_target: True
    input:
        FINAL_TARGETS
