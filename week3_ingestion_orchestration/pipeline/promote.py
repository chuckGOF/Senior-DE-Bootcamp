def run_promote(promoter, run_id, table):
    promoter.promote(f"bronze/_staging/{table}/{run_id}")
