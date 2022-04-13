import duckdb
import glob


CREATE_TABLE = """
CREATE TABLE farmsubsidy(
    country ENUM,
    date DATE,
    recipient_id VARCHAR,
    recipient_name VARCHAR,
    recipient_fingerprint VARCHAR,
    recipient_address VARCHAR,
    recipient_country VARCHAR,
    recipient_url VARCHAR,
    scheme VARCHAR,
    scheme_code VARCHAR,
    scheme_description VARCHAR,
    amount DECIMAL(18, 2),
    currency VARCHAR
);
"""


if __name__ == "__main__":
    cursor = duckdb.connect("farmsubsidy.duckdb")
    cursor.execute("DROP TABLE IF EXISTS farmsubsidy")
    cursor.execute(CREATE_TABLE)
    for fp in glob.glob("./data/cleaned/*.gz"):
        cursor.execute(f"COPY farmsubsidy FROM '{fp}' ( HEADER )")
