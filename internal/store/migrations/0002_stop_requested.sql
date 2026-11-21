-- 0002_stop_requested: a stop request from the API (design.md §3.1, §8).
-- Never edit this file once applied; add 0003_*.sql instead.
--
-- Any controller instance can take a stop request, but only the lease
-- holder drives the run. The API records the request here without a
-- fencing token (it must not draw one: that would fence out the driver),
-- and the driver acts on it and logs a fenced stop_requested event. It is
-- a request, not a state: the run's state still changes only through
-- fenced transitions.
ALTER TABLE runs ADD COLUMN stop_requested_at timestamptz;
