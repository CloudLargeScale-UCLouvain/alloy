INSERT INTO nexmark_q11
SELECT
    B.extra,
    B.auction,
    B.bidder,
    count(*) as bid_count,
    SESSION_START(B.`dateeTime`, INTERVAL '10' SECOND) as starttime,
    SESSION_END(B.`dateeTime`, INTERVAL '10' SECOND) as endtime
FROM bid B
GROUP BY B.extra, B.auction, B.bidder, SESSION(B.`dateeTime`, INTERVAL '10' SECOND);