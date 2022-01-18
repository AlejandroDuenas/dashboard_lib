"""This submodule contains functions that help create the SQL queries needed
for the dashboard library
"""
# ----------------------------- 1. Libraries ----------------------------------
from typing import Union
import pandas as pd
import datetime
import sqlalchemy as sqla
import urllib

# ----------------------------- 2. Classes ------------------------------------
class SQLTranslator(object):
    """This object is used to communicate with SQL Server, through
    queries, and manipulation of the tables of interest for the
    dashboard update process.
    """

    @property
    def engine(self):
        """Generates an engine that connects to SQL Server, specifically
        to the SieT database.
        """
        params = urllib.parse.quote_plus(
            "DRIVER={SQL Server Native Client 11.0};"
            "SERVER=SADGVSQL2K19U\DREP,57201;"
            "DATABASE=SieT;"
            "Trusted_Connection=yes;"
            )
        con_str = "mssql+pyodbc:///?odbc_connect={}".format(params)
        engine = sqla.create_engine(con_str)
        return engine
    
    @property
    def connection(self):
        """Generates a connection to SQL Server, with which the user can
        execute queries.
        """
        engine = self.engine
        conn = engine.connect()
        return conn
        
    def query_date_control(self, date: Union[datetime.datetime, str], 
                        connection=None)->None:
        """Runs a query on the tables that feed the dashboard to delete 
        the information from the input period. This should be used when 
        the updating process fails and must be re-runned.

        Args:
            date (Union[datetime.datetime, str]): date  from which the 
                information will be deleted.
            connection (sql connection/engine): object that connects to 
                SQL Server which is needed to excecute the queries.

        Returns:
            None
        """ 
        if not connection:
            connection = self.connection

        query = f"""
        DELETE
        FROM afectacion_historica_estimada WHERE periodo = '{date}';
        DELETE
        FROM afectacion_saldo_100pbs WHERE periodo = '{date}';
        DELETE
        FROM Calendario_DTC WHERE periodo = '{date}';
        DELETE
        FROM composicion_saldo WHERE Periodo = '{date}';
        DELETE
        FROM df_saldo_anual_cr WHERE mes = '{date}';
        DELETE
        FROM df_saldo_anual_segmentado WHERE Periodo = '{date}';
        DELETE
        FROM diferencia_ingresos_tasaMV WHERE periodo = '{date}';
        DELETE
        FROM estimacion_sensibilidad WHERE Periodo = '{date}';
        DELETE
        FROM ingresos_estimados_tasaMV WHERE periodo = '{date}';
        DELETE
        FROM saldo_usura WHERE periodo = '{date}';
        DELETE
        FROM tasas_usura_implicita_facial WHERE periodo = '{date}';
        """
        _ = connection.execute(query)
        
        print(f"queary_date_control runned for {date}")
    
    def query_comp_balance( self, 
                           date: Union[str, datetime.datetime])-> pd.DataFrame:
        """Generates the balance composition data for the input date,
        extracting the data from SieT..composicions_saldo_adg.

        Args:
            date (Union[str, datetime.datetime]): date for which the
                data will be extracted.

        Returns:
            pd.DataFrame: data frame with the balance composition for
                the input date.
        """
        query = f"""
        SELECT periodo
            , saldo_total
            , saldo_capital
            , saldo_intereses
            , saldo_mora
            , saldo_otros
        FROM composicion_saldo_adg
        WHERE periodo='{date}'; 
        """
        df = pd.read_sql_query(query, con=self.engine)
        return df
    
