# Benchmark a GFA 1.1 graph whose haplotypes are represented by W records.
configfile: "config.yaml"

PATH_FORMAT = "W"

include: "rules/common.smk"


# Define the final target in the entry point so Snakemake selects it when the
# workflow is invoked without an explicit rule name.
localrules: all


rule all:
    default_target: True
    input:
        FINAL_TARGETS
