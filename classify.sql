CREATE TABLE IF NOT EXISTS sms_classifications (
    id SERIAL PRIMARY KEY,
    mobile VARCHAR(20),
    message TEXT,
    is_transactional BOOLEAN
);