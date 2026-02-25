import logging
from pathlib import Path
from week1_basics.config import settings
from week1_basics.src.extract import extract
from week1_basics.src.transform import transform
from week1_basics.src.load import load

from week1_basics.src.transform import transform

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

def run():
    logging.info('Starting ETL job')
    basedir = Path(__file__).parent.parent
    input_path = Path(basedir / settings.input_path)
    output_path = Path(basedir / settings.output_path)

    df = extract(input_path)
    df = transform(df)
    load(df, output_path)

    logging.info('ETL completed successfully')



if __name__ == "__main__":
    run()