"""This submodue contains utility functions needed to do small tasks 
in objects and functions. This functions can also serve during the
dashboard update process.
"""

# ------------------------------ Librarires -----------------------------------
import pandas as pd
from typing import Union
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from .custom_exceptions import *
# ------------------------------- Functions -----------------------------------
def find_pattern(pattern: str, input_list: list)-> Union[str, None]:
    """Search a string inside the input_list that contains the pattern
    introduced as input.

    Args:
        pattern (str): pattern searched inside the list of strings.
        input_list (list): list of strings which is searched for the
            string that contains the input pattern.

    Returns:
        str: string in input_list that contains the input pattern.
    """    
    for string in input_list:
        if pattern in string:
            return string
        else: continue
    print(f'{pattern} was not fount')

    return None

def reduce_memory_usage(df: pd.DataFrame, verbose: bool =True)-> pd.DataFrame:
    """Reduces the meory usage of the inputed data frame by changing the
    dtypes of the columns for less memory-expensive dtypes.

    Args:
        df (pd.DataFrame): data frame which will be optimized.
        verbose (bool, optional): if true, prints the change in memory
            usage of the input data frame. Defaults to True.

    Returns:
        pd.DataFrame: optimized data frame.
    """
    numerics = ["int8", "int16", "int32", "int64", "float16", "float32",
                "float64"]
    start_mem = df.memory_usage().sum()/1024**2

    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == "int":
                if (c_min > np.iinfo(np.int8).min and
                        c_max < np.iinfo(np.int8).max):
                    df[col] = df[col].astype(np.int8)

                elif (c_min > np.iinfo(np.int16).min and
                        c_max < np.iinfo(np.int16).max):
                    df[col] = df[col].astype(np.int16)

                elif (c_min > np.iinfo(np.int32).min and
                        c_max < np.iinfo(np.int32).max):
                    df[col] = df[col].astype(np.int32)

                elif (c_min > np.iinfo(np.int64).min and
                        c_max < np.iinfo(np.int64).max):
                    df[col] = df[col].astype(np.int64)

            else:
                if (c_min > np.finfo(np.float16).min and 
                        c_max < np.finfo(np.float16).max):
                    df[col] = df[col].astype(np.float16)

                elif (c_min > np.finfo(np.float32).min and 
                        c_max < np.finfo(np.float32).max):
                    df[col] = df[col].astype(np.float32)

                else:
                    df[col] = df[col].astype(np.float64)
    end_mem = df.memory_usage().sum() / 1024 ** 2
    if verbose:
        print(f"Initial memory usage = {start_mem:.2f}")
        print(
            "Mem. usage decreased to {:.2f} Mb ({:.1f}% reduction)".format(
                end_mem, 100 * (start_mem - end_mem) / start_mem)
             )
    return df

# --------------------------------- Classes -----------------------------------
class ReferenceDate(object):
    """This object stores methods and attributes to facilitate the
    generation of dates in different formats, and reference dates needed
    for the dashboard update process.
    """
    def __init__(self, ref_date: Union[str, datetime.datetime] = None):
        """
        Args:
            ref_date (Union[str, datetime.datetime], optional): date of
                reference for the period of analysis. If the input is a
                string, the forma must be '%Y-%m'. Defaults to None.
        """
        if not ref_date:
            ref_date = datetime.date.today()
            ref_date = datetime.datetime.combine(
                ref_date, datetime.datetime.min.time()
                )
        else:
            ref_date = datetime.datetime.strptime(ref_date+'-01', '%Y-%m-%d')
        
        self.ref_date = ref_date
    
    def get_format_date(self, int_date: str = 'first_date', 
                        format: str = '%Y-%m-%d')-> str:
        """Depending on the int_date, returns a date as string in the
        format passed as input.

        Args:
            int_date (str, optional): determines which date will be
                transformed to the input string format given. Defaults 
                to 'first_date'. The options are:

                - 'first_date': returns the first date of the month of
                    reference.

                - 'last_date': returns the last date of the mont of
                    reference.

                - 'prev_first_date': returns the first date of the month
                    preceding the reference month.
                    
                - 'prev_last_date': returns the last date of the month
                    preceding the reference month.
                
            format (str, optional): format in which the date will be
                returned as a string. Defaults to '%Y-%m-%d'.

        Returns:
            str: string in the format given as input.
        """
        if int_date=='first_date':
            return self.first_date.strftime(format=format)
        elif int_date=='last_date':
            return self.last_date.strftime(format=format)
        elif int_date=='prev_first_date':
            return self.prev_first_date.strftime(format=format)
        elif int_date=='prev_last_date':
            return self.prev_last_date.strftime(format=format)
        else:
            raise InvalidDate()
    
    @property
    def  last_date(self):
        """Datetime of the last day of the month of interest"""
        last_date = self.ref_date+relativedelta(months=1)-\
            relativedelta(days=self.ref_date.day)
        return last_date

    @property
    def first_date(self):
        """Datetime of the first day of the month of interest"""
        first_date = self.ref_date-relativedelta(days=self.ref_date.day-1)
        return first_date
    
    @property
    def prev_last_date(self):
        """Datetime of the last day of the previous month from the date
        of interest.
        """
        prev_last_date = self.ref_date-relativedelta(days=self.ref_date.day)
        return prev_last_date
    
    @property
    def prev_first_date(self):
        """Datetime of the first day of the previous month from the date
        of interest.
        """
        prev_first_date = self.ref_date-\
            relativedelta(days=self.ref_date.day-1)-relativedelta(months=1)
        return prev_first_date
    
    
