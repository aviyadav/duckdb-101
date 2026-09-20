-- =============================================================================
--  08_group_by_event_type.sql
--  Group on a VARIANT field - the pattern you would use for real GitHub data.
-- =============================================================================

SELECT
    v.type::VARCHAR        AS event_type,
    count(*)               AS events,
    count(v.org.login)     AS with_org,
    count(v.payload.action) AS with_action
FROM lake.test.gh_shred
GROUP BY 1
ORDER BY events DESC, event_type;
