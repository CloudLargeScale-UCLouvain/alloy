INSERT INTO nexmark_q11
SELECT
    B.bidder,
    count(*) as bid_count,
    SESSION_START(B.`dateTime`, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.`dateTime`, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.bidder, SESSION(B.`dateTime`, INTERVAL '10' SECOND);