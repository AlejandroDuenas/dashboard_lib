"""This submodule contains custom exceptions used during the execution
of the TC dashboard update.
"""
class InvalidDate(Exception):
    """Exception raised if there is a invalid int_date."""
    def __init__(self):
        self.message = """
        Invalid int_date value. The only valid values are:
        - 'first_date': returns the first date of the month of
            reference.
        - 'last_date': returns the last date of the mont of
            reference.
        - 'prev_first_date': returns the first date of the month
            preceding the reference month.
        - 'prev_last_date': returns the last date of the month
            preceding the reference month.
        """
        super().__init__(self.message)

    def __str__(self):
        return self.message