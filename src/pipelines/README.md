# Pipelines

Each step of the ETL process lives as a separate `pipeline`. These steps, in theory, are the separate components that are needed to run together to perform logical components of the project (e.g., "sync data", "do ML classifications", etc.).

## `analysis`

Performs data analyses, creates visualizations, etc. (TO BE IMPLEMENTED)

## `annotate`

Performs annotation and validation of user responses. Users respond to our questions on Reddit and we manually annotate and validate these responses.

## `author_phase`

Performs `author phase` of the project (see main README for explanation).

## `classify`

Runs classifications on user comments in order to detect the presence of outrage.

## `observer_phase`

Performs `observer phase` of the project (see main README for explanation)

## `sync`

Syncs comments from Reddit.
