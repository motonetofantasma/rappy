USE DATABASE RAPPIPAY;
USE SCHEMA RAW;

COPY INTO @airflow_rappy_stage/cleaned_events.csv
FROM cleaned_events
FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @airflow_rappy_stage/group_topic_summary.csv
FROM group_topic_summary
FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @airflow_rappy_stage/member_interests.csv
FROM member_interests
FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', COMPRESSION = NONE)
OVERWRITE = TRUE;

COPY INTO @airflow_rappy_stage/group_category_stats.csv
FROM group_category_stats
FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', COMPRESSION = NONE)
OVERWRITE = TRUE;
