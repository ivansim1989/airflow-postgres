import logging

def setup_logging(level=logging.DEBUG):
    """
    Set up logging with the specified log level.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    return logger