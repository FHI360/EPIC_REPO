from logzero import logger, LogFormatter, setup_default_logger
import logzero
import os


class LogFormat:
    def __init__(self, log_file_name, destination_folder):

        self.log_file_name = log_file_name
        self.destination_folder = destination_folder

    def config(self):
        # Create destination folder if it doesn't exist
        try:
            os.makedirs(self.destination_folder, exist_ok=True)
        except OSError as error:
            pass

        # Set a custom formatter
        level_name = "* %(levelname)1s"
        time = "%(asctime)s,%(msecs)03d"
        message = "%(message)s"
        caller = "%(module)s:%(lineno)d"
        log_format = f'%(color)s[{level_name} {time} {caller}] {message}%(end_color)s'
        formatter = LogFormatter(fmt=log_format)

        # Log file path
        output = f"{self.destination_folder}/{self.log_file_name}.log"

        # Set up default logger for logging to a file
        setup_default_logger(logfile=output, formatter=formatter)

        # Return the global logger for use in other classes
        return logger
