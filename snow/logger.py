import logging


def get_logger():
    for logger_name in ('snowflake.snowpark', 'snowflake.connector'):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        logger.addHandler(ch)
        return logger
