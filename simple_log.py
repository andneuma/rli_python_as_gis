import logging


class SimpleLogger:
    logging_status = 0

    def __init__(self, module_name=None):
        self.printmessage = logging.getLogger('logger')
        self._console_handler = logging.StreamHandler()

        self._console_handler.setFormatter(
            logging.Formatter(
                '{module_name}::%(levelname)s: %(message)s'.format(
                    module_name=module_name)))
        self.printmessage.addHandler(logging.StreamHandler())

        SimpleLogger.logging_status = 1

    def set_debug_level(self, value):
        """
        Set level of logging output to either
        ('d', 10): 'debug':
        ('i', 20): 'info'
        ('w', 30): 'warning'
        ('r', 40): 'error'
        ('c', 50): 'critical'
        :param value: Debug level from 10-50
        """
        if value in (10, 'd', 'debug'):
            self.printmessage.setLevel(10)
        elif value in (20, 'i', 'info'):
            self.printmessage.setLevel(20)
        elif value in (30, 'w', 'warning'):
            self.printmessage.setLevel(30)
        elif value in (40, 'e', 'error'):
            self.printmessage.setLevel(40)
        elif value in (50, 'c', 'critical'):
            self.printmessage.setLevel(50)
        else:
            self.printmessage.setLevel(0)
