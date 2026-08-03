import utils

def run_step(step_name, step_function):
    logger = utils.get_logger()
    logger.info(f"Starting {step_name}")
    try:
        result = step_function()
        logger.info(f"Completed {step_name}")
    except Exception as e:
        logger.error(f"Error occurred while running {step_name}: {e}")
        raise
    return result