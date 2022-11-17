# scorecard-to-ssrs-pipeline

bulk_backfill.py is the main function.

percent_clean_scores_section.py is the logic that aggregates fulcrum data into sections. Linear miles are defined at the section level. Percent clean miles at the section level is ratio of clean to total times linear miles.

percent clean score district, boro, and citywide are aggregations of the section level. Percentages clean/filthy are based miles defined at the section level, not counts.
