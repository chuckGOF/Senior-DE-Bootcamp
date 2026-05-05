# Protect against ingestion loops


def validate_watermark_progression(old_wm, new_wm):
    if new_wm <= old_wm:
        raise Exception(f"Watermark did not advance: {old_wm} -> {new_wm}")
