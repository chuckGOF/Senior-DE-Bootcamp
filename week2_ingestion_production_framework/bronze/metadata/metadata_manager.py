class MetadataManager:
    def __init__(self, conn):
        self.conn = conn

    def get_tables(self):
        query = """
            SELECT 
                table_name,
                source_schema,
                primary_key,
                partition_column,
                watermark_column
            FROM metadata.ingestion_tables
            WHERE active = 1
        """

        cursor = self.conn.cursor()
        return cursor.execute(query).fetchall()
    

    def get_watermark(self, table):
        query = """
            SELECT
                watermark_value
            FROM metadata.ingestion_watermarks
            WHERE table_name = ?
        """

        cursor = self.conn.cursor()
        result = cursor.execute(query, table).fetchone()
        return result[0]
    
    
    def update_watermark(self, table, watermark):
        query = """
            UPDATE metadata.ingestion_watermarks
            SET watermark_value = ?, updated_at = CURRENT_TIMESTAMP
            WHERE table_name = ?
        """

        cursor = self.conn.cursor()
        cursor.execute(query, watermark, table)

        self.conn.commit()


    def log_pipeline_run(
        self,
        run_id,
        table,
        start_time,
        end_time,
        rows,
        status,
        error=None
    ):
        query = """
            INSERT INTO metadata.pipeline_runs
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        cursor = self.conn.cursor()
        cursor.execute(
            query,
            run_id,
            table,
            start_time,
            end_time,
            rows,
            status,
            error
        )

        self.conn.commit()