-- Tabla 1: cleaned_events
CREATE OR REPLACE TABLE cleaned_events AS
SELECT
    e.event_id,
    e.event_name,
    e.created,
    e.duration,
    g.group_name,
    g.city AS group_city,
    g.country AS group_country,
    c.C2 AS category_name,
    v.venue_name,
    v.address_1,
    v.city AS venue_city
FROM events e
JOIN groups g ON e.group_id = g.group_id
JOIN venues v ON e.venue_id = v.venue_id
JOIN categories c ON g.category_id = c.C1
WHERE e.duration IS NOT NULL AND e.created IS NOT NULL;

-- Tabla 2: group_topic_summary
CREATE OR REPLACE TABLE group_topic_summary AS
SELECT
    t.topic_id,
    t.topic_name,
    COUNT(gt.group_id) AS group_count
FROM groups_topics gt
JOIN topics t ON gt.topic_id = t.topic_id
GROUP BY t.topic_id, t.topic_name;

-- Tabla 3: member_interests
CREATE OR REPLACE TABLE member_interests AS
SELECT DISTINCT
    m.member_id,
    m.member_name,
    LOWER(TRIM(t.topic_name)) AS normalized_topic_name,
    LOWER(TRIM(m.bio)) AS normalized_bio
FROM members_topics mt
JOIN members m ON mt.member_id = m.member_id
JOIN topics t ON mt.topic_id = t.topic_id;

-- Tabla 4: group_category_stats
CREATE OR REPLACE TABLE group_category_stats AS
SELECT
    c.C2 AS category_name,
    COUNT(DISTINCT g.group_id) AS total_groups,
    COUNT(e.event_id) AS total_events
FROM groups g
JOIN categories c ON g.category_id = c.C1
LEFT JOIN events e ON g.group_id = e.group_id
GROUP BY c.C2;
