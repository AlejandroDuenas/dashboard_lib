"""This submodule contains the main object that process the credit card
(TC) information from the data lake.
"""
#--------------------------------- Libraries ----------------------------------
import pandas as pd
import datatable as dt
import sqlalchemy
import urllib
from typing import Union
import datetime
from .sql_queries import *
from .utils import *

pd.options.mode.chained_assignment = None  # default='warn'

#---------------------------------- Classes -----------------------------------

class ProcesamientoTC(object): 
    def __init__(
        self, comp_saldo_path: str, 
        fact_txc_path: str, 
        tasa_usura: float,
        tasa_usura_t1: float,
        tasa_implicita: float,
        update_date: Union[str, datetime.datetime] = None,
        ):
        """This class contains the methods and attributes needed for the
        updating process of the TC Dashboard.

        Args:
            comp_saldo_path (str): path of the comp_saldo data.
            fact_txc_path (str): path of the fact_txc data.
            tasa_usura (float): usury rate of the update month.
            tasa_usura_t1 (float): usuary rate of the month after the
                update month.
            tasa_implicita (float): implicit rate of the update month.
            update_date (Union[str, datetime.datetime], optional): date
                of the update month. If string, the format must be
                '%Y-%m'. Defaults to None.
        """
        self.comp_saldo_path = comp_saldo_path
        self.fact_txc_path = fact_txc_path
        self.tasa_usura = tasa_usura
        self.tasa_usura_t1 = tasa_usura_t1
        self.tasa_implicita = tasa_implicita
        self.update_date = ReferenceDate(ref_date=update_date)
        self.sql = SQLTranslator()

        # Make sure to clean the unwanted stored data in SQL Server:
        self.sql.query_date_control(
            self.update_date.get_format_date(int_date='last_date')
            )
    
    def balance_composition(self):
        """Computes the total balance, capital balance, interest 
        balance, delinquent balance, and others balance. It also 
        computes the monthly change of these balances and stores all the
        information in SQL Server (SieT..composicion_saldo_adg).
        """       
        saldo_mensual_total = []
        saldo_capital = []
        saldo_interes = []
        saldo_mora = []
        saldo_otros = []
        periodo = []
        
        columns = [
            'nro_producto', 'nro_producto_cifrado', 'tipo_producto', 
            'nro_identificacion','tipo_identificacion', 'codigo_producto', 
            'franquicia', 'clase_cartera', 'cupo_aprobado', 'cupo_disponible', 
            'saldo_capital', 'saldo_capital_empleados', 'saldo_int_cte_activo', 
            'saldo_int_mora', 'saldo_seguros', 'saldo_productivo_cuota_manejo', 
            'saldo_improduct_cuota_manejo', 'saldo_compras', 'compras_mes', 
            'saldo_avances', 'valor_avances_mes', 'saldo_capital_mora', 
            'saldo_otros_cargos', 'saldo_a_favor', 'saldo_capital_usd', 
            'saldo_int_ctes_usd', 'saldo_int_mora_usd', 'saldo_a_favor_dolar', 
            'saldo_total_corte', 'periodo'
            ]

        df_mes = dt.fread(self.comp_saldo_path).to_pandas()
        df_mes.columns = columns
        df_mes = reduce_memory_usage(df_mes)
        
        #saldo total
        saldo_total = (
            df_mes.saldo_capital.values.sum() -
            df_mes.saldo_capital_mora.values.sum() 
            )
        saldo_mensual_total.append(saldo_total)
    
        
        #saldo capital
        saldo_k = (
            df_mes['saldo_avances'].values.sum() 
            + df_mes['saldo_compras'].values.sum() 
            - df_mes['saldo_capital_mora'].values.sum() 
            + df_mes['saldo_a_favor'].values.sum()
            + df_mes['saldo_a_favor_dolar'].values.sum()
            + df_mes['saldo_capital_usd'].values.sum() 
            + df_mes['saldo_improduct_cuota_manejo'].values.sum()
            )
            
        saldo_capital.append(saldo_k)
        
        #saldo interes
        saldo_intereses = (
            df_mes['saldo_int_ctes_usd'].values.sum() 
            + df_mes['saldo_int_cte_activo'].values.sum() 
            + df_mes['saldo_int_mora'].values.sum() 
            + df_mes['saldo_int_mora_usd'].values.sum()
            )
        saldo_interes.append(saldo_intereses)
        
        #saldo mora
        saldo_mora_1 = df_mes.saldo_capital_mora.values.sum()
        saldo_mora.append(saldo_mora_1)
        
        #saldo otros
        saldo_otros_1 = (
            df_mes.saldo_otros_cargos.values.sum() 
            + df_mes.saldo_productivo_cuota_manejo.values.sum() 
            + df_mes.saldo_seguros.values.sum()
            )
        saldo_otros.append(saldo_otros_1)
        
        # periodos
        num_periodo = df_mes.periodo.unique()
        nombre_periodo = num_periodo[0]
        periodo.append(nombre_periodo)
        
        df_composicion_saldo = pd.DataFrame({
            "periodo":periodo,"saldo_total":saldo_mensual_total,
            "saldo_capital":saldo_capital, "saldo_intereses": saldo_interes,
            "saldo_mora":saldo_mora,"saldo_otros":saldo_otros
            })
        df_composicion_saldo.Periodo = pd.to_datetime(
            df_composicion_saldo.Periodo, 
            format= '%Y%m%d'
            )  
        df_composicion_saldo.set_index('periodo', inplace=True)      
                
        # return previous month's balance composition:
        last_month_date = self.update_date.get_format_date("prev_last_date")
        df = self.sql.query_comp_balance(last_month_date).set_index('periodo')
        
        # Validate that the columns are the same and in the same order:
        assert not all(
            [df_composicion_saldo.columns[i]==df.columns[i] for i
            in range(len(df_composicion_saldo.columns))]
            ), "Column names don't correspond with historical data"

        monthly_var_df = df_composicion_saldo/df.values-1
        monthly_var_df.columns = ['var_'+i for i in monthly_var_df.columns]

        df_composicion_saldo = df_composicion_saldo.merge(
            monthly_var_df,
            left_index = True,
            right_index = True
            )

        df.columns = ['objetivos_de_destino_'+i for i in df.columns]
        df.index = df_composicion_saldo.index

        df_composicion_saldo = df_composicion_saldo.merge(
            df, 
            left_index = True,
            right_index  =True
            ).reset_index()
        df_composicion_saldo.to_sql('composicion_saldo_adg', 
                                    con=self.sql.engine, index=False,
                                    if_exists='append')
        
        print('Balance Composition Updated!')
        
        
        
    ''' SECCIÓN 2 '''
    
    def procesar(archivo, nombre_mes):
        # start_time = pd.Timestamp('now') 
            #Selección de columnas
        sel_columnas = ['nro_producto', 'nro_producto_cifrado', 'fecha_transaccion', 'nro_comprobante_transaccion', 'tipo_transaccion', 'cod_transaccion', 'nro_primera_facturacion', 'fecha_primera_facturacion', 'fecha_aplicacion', 'fecha_liquidacion_interes', 'fecha_deposito', 'nro_referencia_universal', 'cod_comercio', 'tipo_comercio', 'tasa_interes', 'modalidad_interes', 'porcentaje_comision_credito', 'valor_transaccion', 'valor_primera_cuota', 'nro_cargos_diferidos', 'valor_propina', 'saldo_actual_trasaccion', 'cuotas_por_facturar', 'valor_act_cuotas_por_facturar', 'valor_ult_cuota', 'estado_transaccion', 'nro_autorizacion', 'fecha_ultimo_pago','nro_interno_puc', 'saldo_ultima_facturacion', 'ultima_cuota_facturada', 'nro_cuotas_por_facturar', 'nro_ultima_facturacion', 'fecha_reversion', 'modalidad_credito', 'valor_cambio', 'valor_tarifa', 'oficina_establecimiento', 'cod_origen_movimiento', 'tipo_producto', 'cod_linea_diferida', 'cod_motivo', 'manejo_periodo_gracia', 'nro_facturacion_transaccion', 'campo_temporal_txt', 'intereses_provisionales_transaccion', 'valor_iva', 'interes_facturado', 'cantidad_cuotas_factura', 'saldo_interes_acumulado', 'plan_pagos_especial', 'plan_amortizacion', 'tasa_interes_plan_pagos', 'nro_tasa_interes', 'cuotas_extra_calendarizadas', 'val_cuotas_extra_calendarizadas', 'cantidad_cuotas_estraordinarias', 'frecuencia_cuotas_extraordinarias', 'dias_periodo_gracia', 'periodo', 'tasa_interes_1']
        # usecols = ['nro_producto','fecha_transaccion','tipo_transaccion','cod_transaccion', 'fecha_primera_facturacion', 'fecha_liquidacion_interes','tasa_interes', 'tasa_interes_1', 'modalidad_interes','valor_transaccion', 'valor_primera_cuota', 'nro_cargos_diferidos','saldo_actual_trasaccion', 'cuotas_por_facturar','valor_act_cuotas_por_facturar', 'valor_ult_cuota','estado_transaccion', 'nro_cuotas_por_facturar', 'nro_ultima_facturacion', 'tipo_producto','periodo']
        # usecols_1 = ['nro_producto','fecha_transaccion','tipo_transaccion','cod_transaccion', 'fecha_primera_facturacion', 'fecha_liquidacion_interes','tasa_interes_1', 'modalidad_interes','valor_transaccion', 'valor_primera_cuota', 'nro_cargos_diferidos','saldo_actual_trasaccion', 'cuotas_por_facturar','valor_act_cuotas_por_facturar', 'valor_ult_cuota','estado_transaccion', 'nro_cuotas_por_facturar', 'nro_ultima_facturacion', 'tipo_producto','periodo']
        usecols = ['nro_producto','fecha_transaccion','tipo_transaccion','cod_transaccion', 'tasa_interes_1', 'valor_transaccion', 'nro_cargos_diferidos','saldo_actual_trasaccion', 'tipo_producto','periodo']
            
        # Especificación del tipo de dato para producto
        dtype = {"nro_producto":str,"nro_producto_cifrado":str}
        size = 500000
            
        # Lectura a trozos del dataframe
        df_chunk = pd.read_csv(archivo, sep=';', names=sel_columnas, dtype=dtype, header=None, usecols=usecols,chunksize=size,
                                  low_memory=False)
            
        # Iteración sobre cada trozo y guardarlo en una lista de df
        chunk_list = []
        for chunk in df_chunk:
            chunk_list.append(chunk)
            
            # Unión de todos los df
        df = pd.concat(chunk_list)
            
        df_1 = df[usecols]
        df_2 = df_1.rename(columns={"tasa_interes_1":"tasa_interes"})
            
        # print(f'{nombre_mes} completo')
        df_3 = df_2[df_2['tipo_transaccion'].isin([1,2,'1','2','C','V'])]
        # print(f'{nombre_mes} con tipo de transacción')
                
        # Comprobar si hay datos que impidan transformar los tipos de datos:
        # Cargos diferidos
        if df_3.iloc[-1:,10:11].isna().values == True:
            df_3 = df_3.iloc[:-1,:]
        # cuotas_por_facturar    
        if df_3.iloc[-1:,12:13].isna().values == True:
            df_3 = df_3.iloc[:-1,:]
        # estado_transaccion    
        if df_3.iloc[-1:,15:16].isna().values == True:
            df_3 = df_3.iloc[:-1,:]
        # nro_cuotas_por_facturar    
        if df_3.iloc[-1:,16:17].isna().values == True:
            df_3 = df_3.iloc[:-1,:]
        # nro_ultima_facturacion    
        if df_3.iloc[-1:,17:18].isna().values == True:
            df_3 = df_3.iloc[:-1,:]
        # periodo     
        if df_3.iloc[-1:,19:20].isna().values == True:
            df_3 = df_3.iloc[:-1,:]  
            
        # Transformar los tipos de datos para cargarlos a SQL Server
        
        df_3.nro_producto = df_3.nro_producto.astype('str')
        df_3.fecha_transaccion = df_3.fecha_transaccion.astype('str')
        df_3.tipo_transaccion = df_3.tipo_transaccion.astype('str')
        df_3.cod_transaccion = df_3.cod_transaccion.astype('str')
        # df_3.fecha_primera_facturacion = df_3.fecha_primera_facturacion.astype('str')        
        # df_3.fecha_liquidacion_interes = df_3.fecha_liquidacion_interes.astype('str')
        df_3.tasa_interes = df_3.tasa_interes.astype('str')
        # df_3.modalidad_interes = df_3.modalidad_interes.astype('str')
        df_3.valor_transaccion = df_3.valor_transaccion.astype('str')
        # df_3.valor_primera_cuota = df_3.valor_primera_cuota.astype('str')
        df_3.nro_cargos_diferidos = df_3.nro_cargos_diferidos.astype('str')        
        df_3.saldo_actual_trasaccion = df_3.saldo_actual_trasaccion.astype('str')
        # df_3.cuotas_por_facturar = df_3.cuotas_por_facturar.astype('str')
        # df_3.valor_act_cuotas_por_facturar = df_3.valor_act_cuotas_por_facturar.astype('str')
        # df_3.valor_ult_cuota = df_3.valor_ult_cuota.astype('str')
        # df_3.estado_transaccion = df_3.estado_transaccion.astype('str')
        # df_3.nro_cuotas_por_facturar = df_3.nro_cuotas_por_facturar.astype('str')        
        # df_3.nro_ultima_facturacion = df_3.nro_ultima_facturacion.astype('str')
        df_3.tipo_producto = df_3.tipo_producto.astype('str')
        df_3.periodo = pd.to_datetime(df_3.periodo, format='%Y%m%d')


        # print(f'Transformación de datos completa para: {nombre_mes}')
            
        df_3 = df_3.reset_index()
        df_3 = df_3.rename(columns={'index':'llave'})
        periodo_1 = df_3.iloc[0][10]
        # periodo_1 = periodo_1[0] 
        periodo_1 = periodo_1.strftime("%Y-%m-%d")
        df_3 = df_3[['llave','nro_producto','fecha_transaccion','tipo_transaccion','cod_transaccion', 'tasa_interes', 'valor_transaccion', 'nro_cargos_diferidos',
                     'saldo_actual_trasaccion', 'tipo_producto','periodo']]
        df_3.llave = df_3.llave.astype('str')
        df_3['periodo'] = periodo_1    
        df_3.periodo = df_3.periodo.astype('str')
        
        
        import sqlalchemy
        import urllib
        
        
        params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=SADGVSQL2K19U\DREP,57201;"
                                         "DATABASE=SieT;"
                                         "Trusted_Connection=yes;")
        
        
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
        
        
        inicio = pd.Timestamp('now')
        df_3.to_sql(f'{nombre_mes}', con=engine, if_exists='replace', index=False, chunksize=500000)
        print(pd.Timestamp('now')-inicio)
            
        '''
        #Crear conexión con repositorio SQL para crear tabla
        import pyodbc as sql
                        
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")
                
        cursor = con.cursor()
                    
                        
        query_createTable = """CREATE TABLE %s (
                                                llave varchar(50) NULL,
                                                nro_producto varchar(50) NULL, 
                                                fecha_transaccion varchar(50) NULL,  
                                                tipo_transaccion varchar(50) NULL, 
                                                cod_transaccion varchar(50) NULL,  
                                                fecha_primera_facturacion varchar(50) NULL, 
                                                fecha_liquidacion_interes varchar(50) NULL,  
                                                tasa_interes varchar(50) NULL,  
                                                modalidad_interes varchar(50) NULL, 
                                                valor_transaccion varchar(50) NULL,  
                                                valor_primera_cuota varchar(50) NULL, 
                                                nro_cargos_diferidos varchar(50) NULL,
                                                saldo_actual_trasaccion varchar(50) NULL,
                                                cuotas_por_facturar varchar(50) NULL,
                                                valor_act_cuotas_por_facturar varchar(50) NULL, 
                                                valor_ult_cuota varchar(50) NULL, 
                                                estado_transaccion varchar(50) NULL,  
                                                nro_cuotas_por_facturar varchar(50) NULL,
                                                nro_ultima_facturacion varchar(50) NULL, 
                                                tipo_producto varchar(50) NULL,  
                                                periodo varchar(50) NULL) """% (nombre_mes)
                
        cursor = con.cursor()
        cursor.execute(query_createTable)
        con.commit()
        # print(cursor.rowcount, f"Tabla {nombre_mes} creada satisfactoriamente")
        cursor.close()
        
        import pyodbc as sql   
        # Crear conexión con repositorio SQL insertar datos
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        for index, row in df_3.iterrows():
            query_insert = """INSERT INTO %s ("llave","nro_producto", "fecha_transaccion", "tipo_transaccion","cod_transaccion", "fecha_primera_facturacion","fecha_liquidacion_interes", "tasa_interes", "modalidad_interes","valor_transaccion", "valor_primera_cuota", "nro_cargos_diferidos","saldo_actual_trasaccion", "cuotas_por_facturar","valor_act_cuotas_por_facturar", "valor_ult_cuota","estado_transaccion", "nro_cuotas_por_facturar","nro_ultima_facturacion", "tipo_producto", "periodo") 
                                       VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                                       """% (nombre_mes)
            cursor.execute(query_insert, [row.llave, row.nro_producto , row.fecha_transaccion, row.tipo_transaccion, row.cod_transaccion, row.fecha_primera_facturacion, row.fecha_liquidacion_interes, row.tasa_interes, row.modalidad_interes, row.valor_transaccion, row.valor_primera_cuota, row.nro_cargos_diferidos, row.saldo_actual_trasaccion, row.cuotas_por_facturar, row.valor_act_cuotas_por_facturar, row.valor_ult_cuota, row.estado_transaccion, row.nro_cuotas_por_facturar, row.nro_ultima_facturacion, row.tipo_producto, row.periodo])
    
        con.commit()
        # print(cursor.rowcount, "Registros insertados satisfactoriamente en SieT..composicion_saldo")
        cursor.close()
        # elapsed_time = pd.Timestamp('now') - start_time
        # print(f'Tiempo de procesamiento para el archivo {archivo} fue de ',elapsed_time)
        '''       
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()
        
        df_4 = df_3[['periodo']]
        df_4 = df_4.iloc[0][0]

        query_insert = """INSERT INTO Calendario_DTC
                            ("periodo") 
                            VALUES (?) """
                            
        cursor.execute(query_insert, df_4)
    
        con.commit()
        cursor.close()
        
        del df
        del df_1
        del df_2
        del df_3


        
        
    '''SECCIÓN 3'''
    
    def tasas_originales(mes_base,num_mes,num_anio):
        
        import pyodbc as sql
        con = sql.connect(
            "DRIVER={SQL Server Native Client 11.0};"
            "SERVER=SADGVSQL2K19U\DREP,57201;"
            "Trusted_Connection=yes;"
        )
    
        cursor = con.cursor()
        
        anio_final = num_anio - 10        
        # anio_final = 2021
        numero_mes = num_mes
        numero_anio = num_anio
    
        df_complete_1 = []   
        while numero_mes >= 1 and numero_anio >= anio_final:     
            for i in range(0,num_mes):
    
                sql_1 = ''' 
                        SELECT p1.llave, p2.FECHA, p1.nro_producto, p2.NEGOCIO, p1.fecha_transaccion, p1.tipo_transaccion, p1.cod_transaccion, p1.nro_cargos_diferidos, 
                        p1.tasa_interes, p1.valor_transaccion, p1.saldo_actual_trasaccion, p1.tipo_producto,
                        p2.IDENTIFICACION, P1.periodo,
                        CASE CAST(p1.nro_cargos_diferidos AS FLOAT)
                            WHEN 1 THEN 0
                            ELSE p2.TASA_EA
                            END as TASA_EA   
                    
                    FROM SieT..[%s] AS p1
                
                    INNER JOIN Maestro_Saldos..Maestro_Facturacion AS p2
                    
                    ON p1.[nro_producto] = p2.NEGOCIO 
                    AND P1.[valor_transaccion] = p2.VALOR_TRANSACCION
                    AND p1.[nro_cargos_diferidos] = p2.CUOTAS_DIFERIDAS
                    AND p2.CODIGO_TRANSACCION = p1.cod_transaccion 
                    AND p2.CODIGO_PRODUCTO = p1.tipo_producto
                    AND p1.[fecha_transaccion] = p2.FECHA_TRANSACCION
                    WHERE p2.FECHA = '%s-%s-01'
                    ; '''% (mes_base, numero_anio, numero_mes)
                print(mes_base, numero_anio, numero_mes)
                
                df_1 = pd.read_sql_query(sql_1, con = con)
                
                df_complete_1.append(df_1)
                
                # print(f'Escaneo mes para tasas originales en mes {numero_mes} y año {numero_anio} completo')
                
                numero_mes = numero_mes - 1
                
                if numero_mes == 0:
                    numero_anio = numero_anio - 1
                    numero_mes = 12
                    
        
        df_complete = pd.concat(df_complete_1)
        # print(f'1/3 Escaneo de tasas originales completo para {mes_base}')
        # print('* ' * 25)    

        df_complete.nro_cargos_diferidos = df_complete.nro_cargos_diferidos.astype('float')
        df_complete.tipo_transaccion = df_complete.tipo_transaccion.astype('float')
        # df_complete.nro_cuotas_por_facturar = df_complete.nro_cuotas_por_facturar.astype('int')
        df_complete.tasa_interes = df_complete.tasa_interes.astype('float')
        df_complete.TASA_EA = df_complete.TASA_EA.astype('float')
        df_complete.valor_transaccion = df_complete.valor_transaccion.astype('float')
        df_complete.saldo_actual_trasaccion = df_complete.saldo_actual_trasaccion.astype('float')
        # df_complete.valor_ult_cuota = df_complete.valor_ult_cuota.astype('float')
        # df_complete.estado_transaccion = df_complete.estado_transaccion.astype('int')
        # df_complete.nro_ultima_facturacion = df_complete.nro_ultima_facturacion.astype('int')
        # df_complete.periodo = pd.to_datetime(df_complete.periodo, format='%Y%m%d')
        
        # print('2/3 Tipos de datos modificados satisfactoriamente')
        # print('* ' * 25)
        
    
        # return df_complete
        import pyodbc as sql
        con = sql.connect(
            "DRIVER={SQL Server Native Client 11.0};"
            "SERVER=SADGVSQL2K19U\DREP,57201;"
            "Trusted_Connection=yes;"
        )
    
        cursor = con.cursor()
        
        sql_1 = ''' SELECT *                    
                    FROM SieT..[%s]; '''% (mes_base)
                
        df_mes_ta = pd.read_sql_query(sql_1, con = con)  
        con.commit()
        cursor.close()     

        df_mes_ta.nro_cargos_diferidos = df_mes_ta.nro_cargos_diferidos.astype('float')
        # df_mes_ta.nro_cuotas_por_facturar = df_mes_ta.nro_cuotas_por_facturar.astype('int')
        df_mes_ta.tasa_interes = df_mes_ta.tasa_interes.astype('float')        
        df_mes_ta.valor_transaccion = df_mes_ta.valor_transaccion.astype('float')
        df_mes_ta.saldo_actual_trasaccion = df_mes_ta.saldo_actual_trasaccion.astype('float')
        # df_mes_ta.valor_ult_cuota = df_mes_ta.valor_ult_cuota.astype('float')
        # df_mes_ta.estado_transaccion = df_mes_ta.estado_transaccion.astype('int')
        # df_mes_ta.nro_ultima_facturacion = df_mes_ta.nro_ultima_facturacion.astype('int')
        # df_mes_ta.periodo = pd.to_datetime(df_mes_ta.periodo, format='%Y%m%d')
        
        
        df_mes_to = df_complete
        
        df_mes_to_sinDuplicados = df_mes_to.drop_duplicates(subset=['llave'])
        
        cruce_mes_left = pd.merge(df_mes_ta, df_mes_to_sinDuplicados, how='left', on='llave', indicator=True, validate=('1:1'))
        
        missing = cruce_mes_left[cruce_mes_left['TASA_EA'].isna()]
        complete = cruce_mes_left[~cruce_mes_left['FECHA'].isna()]
        missing['TASA_EA'] = missing['tasa_interes_x'].apply(lambda x: round((((((x/100)+1)**12)-1)*100),2)) 
        
        cruce_mes_left = pd.concat([complete, missing])

        columnas=  ['llave', 'nro_producto_x', 'fecha_transaccion_x', 'tipo_transaccion_x','cod_transaccion_x', 'tasa_interes_x',  'TASA_EA', 'valor_transaccion_x', 'nro_cargos_diferidos_x',
                    'saldo_actual_trasaccion_x', 'tipo_producto_x', 'periodo_x']
        cruce_mes_left = cruce_mes_left[columnas]
        
        cruce_mes_left = cruce_mes_left.rename(columns={'nro_producto_x':'nro_producto', 'fecha_transaccion_x':'fecha_transaccion', 'tipo_transaccion_x':'tipo_transaccion', 'cod_transaccion_x':'cod_transaccion',
                                                        'tasa_interes_x':'tasa_interes',   'valor_transaccion_x':'valor_transaccion','nro_cargos_diferidos_x':'nro_cargos_diferidos', 
                                                        'saldo_actual_trasaccion_x':'saldo_actual_trasaccion', 'tipo_producto_x':'tipo_producto', 'periodo_x':'periodo'})
        cruce_mes_left.TASA_EA = cruce_mes_left.TASA_EA.round(1)
                
        # print('Resultado: ',cruce_mes_left.saldo_actual_trasaccion.sum())
        # print('Debe dar: ',df_mes_ta.saldo_actual_trasaccion.sum())
        # print('Diferencia: ',round(df_mes_ta.saldo_actual_trasaccion.sum() - cruce_mes_left.saldo_actual_trasaccion.sum(),2))
        
        return cruce_mes_left
    
    
    
    
    ''' SECCIÓN 4 '''
    
    def segmentacion_saldo(df):
        '''
        Lee un mes de transacciones específico y calcula el saldo para cada tipo de transacción, sea esta compra de cartera, 
        totaleros, compras y avances.
        Genera una lista de datos calculados para cada filtro.
        ''' 
        # print('Ejecutando la segmentación de saldo. Espere un momento...')
        # Cargue de los datos
        df_mes_input = df
        df_mes_input['interes_ea'] = (((((df_mes_input['tasa_interes']/100)+1)**12)-1)*100).round(2)
    
    
        #Compra de cartera
        saldo_compraCartera_1 = []
        compraCartera_mes = df_mes_input[df_mes_input['cod_transaccion'].isin(['0I','1C','1D','4I','6H','6I','6K','6L','6V','7A','7B','9Q','9S','A0','AW','BW','D0','DU','E8','EB','F7','F8','H7','H8','HK','HL','I7','I8','SE'])]
        compraCartera = compraCartera_mes.reset_index(drop=True)
        saldo_compraCartera = compraCartera.saldo_actual_trasaccion.sum()
        saldo_compraCartera_1.append(saldo_compraCartera)
    
        # Filtrando la base, quitando los de compra de cartera
        df_mes_1 = df_mes_input[~df_mes_input['cod_transaccion'].isin(['0I','1C','1D','4I','6H','6I','6K','6L','6V','7A','7B','9Q','9S','A0','AW','BW','D0','DU','E8','EB','F7','F8','H7','H8','HK','HL','I7','I8','SE'])]
        df_mes_1 = df_mes_1.reset_index(drop=True)
    
        # Identificación de totaleros
        saldo_totaleros_1 = []
        totaleros = df_mes_1[df_mes_1['nro_cargos_diferidos'].isin([1])]
        saldo_totaleros = totaleros.saldo_actual_trasaccion.sum()
        saldo_totaleros_1.append(saldo_totaleros)
    
        # filtrando la base para quitar los totaleros
        df_mes_2 = df_mes_1[~df_mes_1['nro_cargos_diferidos'].isin([1])]
    
        # Identificación de compras
        saldo_compras_1 = []
        compras_mes = df_mes_2[df_mes_2['tipo_transaccion'].isin([1,'1', 'C'])]
        compras_mes = compras_mes.reset_index(drop=True)
        saldo_compras = compras_mes.saldo_actual_trasaccion.sum()
        saldo_compras_1.append(saldo_compras)
    
        # Identificación de avances
        saldo_avances_1 = []
        avances_mes = df_mes_2[df_mes_2['tipo_transaccion'].isin([2, '2', 'V'])]
        avances_mes = avances_mes.reset_index(drop=True)
        saldo_avances = avances_mes.saldo_actual_trasaccion.sum()
        saldo_avances_1.append(saldo_avances)
        
        # periodos
        periodo = []
        num_periodo = df_mes_input.periodo.unique()
        nombre_periodo = num_periodo[0]
        periodo.append(nombre_periodo)
        
        # periodo
        df_periodo = pd.DataFrame({"Periodo":periodo})
        
        # saldo compra de cartera
        df_saldo_compraCartera = pd.DataFrame({"Saldo Compra de Cartera":saldo_compraCartera_1})
        
        # saldo totaleros
        df_saldo_totaleros = pd.DataFrame({"Saldo Totaleros":saldo_totaleros_1})
        
        # saldo compras
        df_saldo_compras = pd.DataFrame({"Saldo Compras":saldo_compras_1})
        
        # saldo avances
        df_saldo_avances = pd.DataFrame({"Saldo Avances":saldo_avances_1})
    
        df_saldo_anual = pd.concat([df_periodo, df_saldo_compraCartera, df_saldo_totaleros, df_saldo_compras, df_saldo_avances], axis='columns')
        
        df_saldo_anual["Saldo Compra de Cartera"] =df_saldo_anual["Saldo Compra de Cartera"].astype('str')
        df_saldo_anual["Saldo Totaleros"] = df_saldo_anual["Saldo Totaleros"].astype('str')
        df_saldo_anual["Saldo Compras"] = df_saldo_anual["Saldo Compras"].astype('str')
        df_saldo_anual["Saldo Avances"] = df_saldo_anual["Saldo Avances"].astype('str')
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        for index, row in df_saldo_anual.iterrows():
            query_insert = """INSERT INTO df_saldo_anual_segmentado 
                            ("Periodo","Saldo Compra de Cartera", "Saldo Totaleros", "Saldo Compras","Saldo Avances") 
                            VALUES (?,?, ?, ?, ?) 
                                       """
            cursor.execute(query_insert, [row["Periodo"], row["Saldo Compra de Cartera"], row["Saldo Totaleros"],
                                          row["Saldo Compras"], row["Saldo Avances"]])
    
        con.commit()
        # print(cursor.rowcount, "Registros insertados satisfactoriamente en SieT..df_saldo_anual_segmentado")
        cursor.close()
        
        # print('Segementación de saldo finalizada correctamente.')
        # print('* '*25)
        
        return df_saldo_anual
    
    
    
    ''' SECCIÓN 5 '''
    
    def tasas_usura_implicita_facial(df_mes_input_facial, usura, implicita):
        import numpy as np
        # start_time = pd.Timestamp('now') 
        # print('Ejecutando calculo Facial y cargando de tasas: Usura, Implícita, espere un momento...')
        # print('\n ')
        # Calculo tasas facial
        tasa_facial = []
        periodo = []
        columnas = ['tasa_interes', 'TASA_EA', 'saldo_actual_trasaccion','periodo']
        df_mes = df_mes_input_facial[columnas]
        df_mes['interes_ea'] = (((((df_mes['tasa_interes']/100)+1)**12)-1)*100).round(2)
        df_mes = df_mes.rename(columns={"TASA_EA":"tasa_ea_original"})
        df_mes.tasa_ea_original = (df_mes.tasa_ea_original).round(2)
        df_mes.interes_ea = (df_mes.interes_ea).round(2)
        df_mes_tasas = df_mes[['interes_ea']]
        df_mes_saldo_porTxc = df_mes[['saldo_actual_trasaccion']]
        df_mes_saldoTotal = df_mes.saldo_actual_trasaccion.sum()
        periodo_mes = df_mes.periodo.unique()
        periodo_mes = periodo_mes[0]
        
        factor_pond = []
        for saldo_porTxc in df_mes_saldo_porTxc.saldo_actual_trasaccion:
            factor = saldo_porTxc / df_mes_saldoTotal
            factor_pond.append(factor)
        
        df_mes_tasas = df_mes_tasas.interes_ea.to_list()
        
        prom_pond = np.multiply(factor_pond, df_mes_tasas)
        tasa_facial_mes = round(sum(prom_pond),2)
        tasa_facial.append(tasa_facial_mes)
        periodo.append(periodo_mes)
                
        tasa_facial = pd.DataFrame({'periodo':periodo,'tasa_usura':usura,'tasa_implicita':implicita,'tasa_facial':tasa_facial})
            
        # print('Calculo de tasa facial completado')
        # print('* ' * 25)
            
        import pyodbc as sql
        
        con = sql.connect(
            "DRIVER={SQL Server Native Client 11.0};"
            "SERVER=SADGVSQL2K19U\DREP,57201;"
            "Trusted_Connection=yes;"
        )
    
        cursor = con.cursor()
        
        sql_1 = ''' SELECT *                    
                    FROM SieT..tasas_usura_implicita_facial; '''
                
        df_tasas = pd.read_sql_query(sql_1, con = con)  
        con.commit()
        cursor.close() 
        
        df_tasas = df_tasas.iloc[-1:]
        
        df_tasas = df_tasas[['periodo','tasa_usura','tasa_implicita','tasa_facial']]
        df_tasas_columns = df_tasas.columns
        for i in df_tasas_columns[1:]:
            df_tasas[i] = df_tasas[i].astype('float')
            

        df_tasas = pd.concat([df_tasas,tasa_facial])
        df_tasas = df_tasas.reset_index(drop=True)
        
        rango_columnas = df_tasas.shape[1]
        z = 1
        variacion_tasas = []
        objetivos_destino = []
        columns = df_tasas.columns
        
        for i in range(1,rango_columnas):
        
            rango_filas = df_tasas.shape[0]
            num_fila = 0
            lista_variacion = [0]
            nom_column = columns[i]
            valor_anterior = [0]
               
            if num_fila < rango_filas:
                x = 0
                y = 1
                for i in range(0,rango_filas-1):
                    anterior = float(df_tasas.iloc[x][z])
                    actual = float(df_tasas.iloc[y][z])
                    variacion = round(((actual - anterior) / anterior), 5)
                    lista_variacion.append(variacion)
                    valor_anterior.append(anterior)
                    x += 1
                    y += 1
                    num_fila += 1
            z +=1
            
            var_saldo_columna = pd.DataFrame({f'Var_{nom_column}':lista_variacion})
            df_objetivos_destino = pd.DataFrame({f'Objetivos de destino_{nom_column}':valor_anterior})
            variacion_tasas.append(var_saldo_columna)
            objetivos_destino.append(df_objetivos_destino)
            
        var_saldos = pd.concat(variacion_tasas,  axis='columns')
        objetivos_destino = pd.concat(objetivos_destino, axis='columns')
        variacion_objetivos_tasas = pd.concat([var_saldos, objetivos_destino], axis='columns')
        df_tasas = pd.concat([df_tasas,variacion_objetivos_tasas],axis='columns')
        df_tasas = df_tasas.iloc[-1:]
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        for index, row in df_tasas.iterrows():
            query_insert = """INSERT INTO tasas_usura_implicita_facial 
                            ("periodo","tasa_usura","tasa_implicita","tasa_facial",
                             "Var_tasa_usura","Var_tasa_implicita","Var_tasa_facial",
                             "Objetivos de destino_tasa_usura","Objetivos de destino_tasa_implicita",
                             "Objetivos de destino_tasa_facial") 
                            VALUES (?,?, ?, ?, ?,?,?, ?, ?, ?) 
                                       """
            cursor.execute(query_insert, [row["periodo"], row["tasa_usura"], row["tasa_implicita"],
                                          row["tasa_facial"], row["Var_tasa_usura"], 
                                          row["Var_tasa_implicita"], 
                                          row["Var_tasa_facial"], 
                                          row["Objetivos de destino_tasa_usura"], 
                                          row["Objetivos de destino_tasa_implicita"], 
                                          row["Objetivos de destino_tasa_facial"]])
    
        con.commit()
        cursor.close()
        # print('Cargue de tasas: Usura, Implícita completado')
        # print('* ' * 25)
        # elapsed_time = pd.Timestamp('now') - start_time
        # print(f"Tiempo de procesamiento tasas_usura_implicita_facial: {elapsed_time}")
        return df_tasas
    
    
    
    
    ''' SECCIÓN 6 '''
    
    def saldo_to_mes(df_mes_input):
        # print('Ejecutando procesamiento para saldos por rango de tasas, espere un momento...')
        # print('\n')
        # start_time = pd.Timestamp('now') 
        
        import numpy as np
        ranges = [np.NINF, 0, 1,11,21,24,26,26.5,27,27.5,28,29,30,np.inf]
        grupo_tasas = ['a. 0%','b. 0.1% - 0.9%', 'c. 1% - 10.9%', 'd. 11% - 20.9%','e. 21% - 23.9%','f. 24% - 25.9%','g. 26% - 26.4%','h. 26.5% - 26.9%', 'i. 27% - 27.4%', 'j. 27.5% - 27.9%', 'k. 28% - 28.9%', 'l. 29% - 29.9%', 'm. >30%']    
            
        df_to = df_mes_input
        df_to = df_to.rename(columns={"TASA_EA":"tasa_ea_original"})
        df_to['interes_ea'] = (((((df_to['tasa_interes']/100)+1)**12)-1)*100).round(2)
        periodo = df_to.periodo.unique()
        periodo = periodo[0]
        # periodo = periodo.strftime("%Y-%m-%d")
    
        saldo_sr_to = df_to.groupby(by=['tasa_ea_original']).agg({'saldo_actual_trasaccion':'sum'})
        saldo_sr_to = saldo_sr_to.sort_values(by='saldo_actual_trasaccion',ascending=False)
        saldo_sr_to = saldo_sr_to.reset_index()
        saldo_sr_to.tasa_ea_original = (saldo_sr_to.tasa_ea_original).round(2)
        saldo_sr_to['mes'] = periodo
        saldo_sr_to['tipo_tasa'] = 'Tasa original'
        
        saldo_sr_ta = df_to.groupby(by=['interes_ea']).agg({'saldo_actual_trasaccion':'sum'})
        saldo_sr_ta = saldo_sr_ta.sort_values(by='saldo_actual_trasaccion',ascending=False)
        saldo_sr_ta = saldo_sr_ta.reset_index()
        saldo_sr_ta.interes_ea = (saldo_sr_ta.interes_ea).round(2)
        saldo_sr_ta['mes'] = periodo
        saldo_sr_ta['tipo_tasa'] = 'Tasa actual'
        
        saldo_cr_to = saldo_sr_to
        saldo_cr_to['rango_tasa_original'] = pd.cut(saldo_cr_to['tasa_ea_original'], bins=ranges, labels= grupo_tasas)
        saldo_cr_to = saldo_cr_to.groupby(by=['rango_tasa_original']).agg({'saldo_actual_trasaccion':'sum'})
        saldo_cr_to = saldo_cr_to.sort_values(by='saldo_actual_trasaccion', ascending=False)
    
        saldo_cr_to = saldo_cr_to.reset_index()
        saldo_cr_to = saldo_cr_to.sort_values(by='rango_tasa_original')
        
        saldo_cr = saldo_cr_to.reset_index(drop=True)
        saldo_cr['mes'] = periodo
        saldo_sr = saldo_sr_to
        # return saldo_cr, saldo_sr_to, saldo_sr_ta
       
        saldo_anual_rango = []
        saldo_anual_sinRango = []
        saldo_anual_sinRango_ta = []
        fs_column_cr = []
        fs_column_sr = []
        fs_column_sr_ta = []
        
        #genera dataframe según rango de tasa
        primera_columna = saldo_cr[['rango_tasa_original']]
        saldo_cr = saldo_cr[['saldo_actual_trasaccion','mes']]
        fs_column_cr.append(primera_columna)
        saldo_anual_rango.append(saldo_cr)
        
        # genera dataframe sin rango de tasa original
        primera_columna_sr = saldo_sr.iloc[:,0] 
        saldo_sr = saldo_sr[['saldo_actual_trasaccion','mes','tipo_tasa']]
        fs_column_sr.append(primera_columna_sr)  
        saldo_anual_sinRango.append(saldo_sr)
        
        # genera dataframe sin rango de tasa actual
        primera_columna_sr_ta = saldo_sr_ta.iloc[:,0] 
        saldo_sr_ta = saldo_sr_ta[['saldo_actual_trasaccion','mes','tipo_tasa']]
        fs_column_sr_ta.append(primera_columna_sr_ta)  
        saldo_anual_sinRango_ta.append(saldo_sr_ta)
            
        primera_columna_rango = pd.concat(fs_column_cr)
        primera_columna_rango = primera_columna_rango.reset_index(drop=True)
        saldo_rango = pd.concat(saldo_anual_rango)
        saldo_rango = saldo_rango.reset_index(drop=True)
        saldo_to_anual_rango = pd.concat([primera_columna_rango, saldo_rango], axis='columns')
        
        primera_columna_sinRango = pd.concat(fs_column_sr)
        primera_columna_sinRango = primera_columna_sinRango.reset_index(drop=True)
        saldo_sinRango = pd.concat(saldo_anual_sinRango)
        saldo_sinRango = saldo_sinRango.reset_index(drop=True)
        saldo_to_anual_sinRango = pd.concat([primera_columna_sinRango, saldo_sinRango], axis='columns')  
        
        primera_columna_sinRango_ta = pd.concat(fs_column_sr_ta)
        primera_columna_sinRango_ta = primera_columna_sinRango_ta.reset_index(drop=True)
        saldo_sinRango_ta = pd.concat(saldo_anual_sinRango_ta)
        saldo_sinRango_ta = saldo_sinRango_ta.reset_index(drop=True)
        saldo_ta_anual_sinRango = pd.concat([primera_columna_sinRango_ta, saldo_sinRango_ta], axis='columns')  
  
        saldo_ta_anual_sinRango = saldo_ta_anual_sinRango.rename(columns={"interes_ea":"interes"})
        saldo_to_anual_sinRango = saldo_to_anual_sinRango.rename(columns={"tasa_ea_original":"interes"})
        
        saldo_sinRango = pd.concat([saldo_ta_anual_sinRango, saldo_to_anual_sinRango])
        
        saldo_ta_anual_conRango = saldo_ta_anual_sinRango
        saldo_to_anual_conRango = saldo_to_anual_sinRango
        
        saldo_ta_anual_conRango['rango_tasa'] = pd.cut(saldo_ta_anual_sinRango['interes'], bins=ranges, labels= grupo_tasas)
        saldo_to_anual_conRango['rango_tasa'] = pd.cut(saldo_to_anual_sinRango['interes'], bins=ranges, labels= grupo_tasas)
        
        saldo_conRango = pd.concat([saldo_ta_anual_conRango, saldo_to_anual_conRango])
        # saldo_conRango.mes = pd.to_datetime(saldo_conRango.mes, format= '%Y%m%d')
        
        saldo_conRango.saldo_actual_trasaccion = saldo_conRango.saldo_actual_trasaccion.astype('str')
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        for index, row in saldo_conRango.iterrows():
            query_insert = """INSERT INTO df_saldo_anual_cr 
                            ("interes","saldo_actual_trasaccion","mes","tipo_tasa",
                              "rango_tasa") 
                            VALUES (?,?, ?, ?, ?) 
                                        """
            cursor.execute(query_insert, [row["interes"], row["saldo_actual_trasaccion"], row["mes"],
                                          row["tipo_tasa"], row["rango_tasa"]])
    
        con.commit()
        cursor.close()
        # print('Procesamiento de Rango de Tasas completado satisfactoriamente')
        # print('\n')
        # elapsed_time = pd.Timestamp('now') - start_time
        # print(f"Tiempo de procesamiento tasas_usura_implicita_facial: {elapsed_time}")
        # print('* ' * 25)
        
        return saldo_sinRango
    
    
    
    
    ''' SECCIÓN 7 '''
    
    def afectacion_100pbs(tasa_usura, df_mes_input):
    
        # start_time  = pd.Timestamp('now')
        
        saldo_expuesto = []
        impacto_pyg = []
        tipo_variacion = []
        rango = []
    
        columnas = ['saldo_actual_trasaccion','tasa_interes','TASA_EA','periodo']
        df = df_mes_input[columnas]
        df = df.rename(columns={"TASA_EA":"tasa_ea_original"})
        df['interes_ea'] = (((((df['tasa_interes']/100)+1)**12)-1)*100).round(2)
        periodo = df.periodo.unique()
        periodo = periodo[0]
        
        df_mes = df[['saldo_actual_trasaccion','tasa_ea_original','interes_ea']]
        
        tasa_usura = tasa_usura
        
        # Usura aumenta 100 pbs
        
        usura_final = tasa_usura + 1
        df_exp_mas100pbs_usura = df_mes[df_mes['tasa_ea_original'] > tasa_usura]
        df_exp_mas100pbs_usura = df_exp_mas100pbs_usura.reset_index(drop=True)
        
        tasa_final = []
        for tasa_original in df_exp_mas100pbs_usura['tasa_ea_original']:
            if tasa_original <= usura_final:
                tasa_final.append(tasa_original)
            elif usura_final < tasa_original:
                tasa_final.append(usura_final)
                
        df_tasa_final = pd.DataFrame({"tasa_final":tasa_final})
        
        diferencia_de_tasas  = tasa_final - df_exp_mas100pbs_usura['interes_ea']
        
        diferencia_de_tasas = pd.DataFrame({"diferencia_tasas": (df_tasa_final['tasa_final'] - df_exp_mas100pbs_usura['interes_ea']) / 100})
        
        df_calculo_impacto = pd.concat([df_exp_mas100pbs_usura,diferencia_de_tasas], axis='columns')
        df_calculo_impacto['impacto_variacion_Npbs'] = df_calculo_impacto['saldo_actual_trasaccion'] * df_calculo_impacto['diferencia_tasas']
        
        impacto_pyg_mas100pbs = df_calculo_impacto.impacto_variacion_Npbs.sum()
        saldo_exp_mas100pbs_usura = df_exp_mas100pbs_usura.saldo_actual_trasaccion.sum()
        
        saldo_expuesto.append(round(saldo_exp_mas100pbs_usura))
        impacto_pyg.append(round(impacto_pyg_mas100pbs))
        tipo_variacion.append('sube 100 Pbs')
        rango.append(periodo)
        
        # Usura disminuye 100 pbs
        
        usura_final_bajada = tasa_usura - 1
        
        df_exp_menos100pbs_usura = df_mes[df_mes['interes_ea'] >= usura_final_bajada]
        df_exp_menos100pbs_usura = df_exp_menos100pbs_usura.reset_index(drop=True)
        saldo_exp_menos100pbs_usura = df_exp_menos100pbs_usura.saldo_actual_trasaccion.sum()
        impacto_pyg_menos100pbs = (saldo_exp_menos100pbs_usura * 0.01) * (-1)
        
        saldo_expuesto.append(round(saldo_exp_menos100pbs_usura, 2))
        impacto_pyg.append(round(impacto_pyg_menos100pbs,2))
        tipo_variacion.append('baja 100 Pbs')
        rango.append(periodo)
           
        afectacion_saldo = pd.DataFrame({"periodo":tipo_variacion})
        afectacion_saldo["periodo"] = rango
        afectacion_saldo["saldo_expuesto"] = saldo_expuesto
        afectacion_saldo["impacto_pyg"] = impacto_pyg
        afectacion_saldo["tipo_variacion"] = tipo_variacion
        
        afectacion_saldo["saldo_expuesto"] = afectacion_saldo["saldo_expuesto"].astype('str')
        afectacion_saldo["impacto_pyg"] = afectacion_saldo["impacto_pyg"].astype('str')
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        for index, row in afectacion_saldo.iterrows():
            query_insert = """INSERT INTO afectacion_saldo_100pbs 
                            ("periodo","saldo_expuesto","impacto_pyg","tipo_variacion") 
                            VALUES (?,?, ?, ?) 
                                       """
            cursor.execute(query_insert, [row["periodo"], row["saldo_expuesto"],
                                          row["impacto_pyg"], row["tipo_variacion"]])
    
        con.commit()
        cursor.close()
        # print('Procesamiento: afetación 100 pbs usura completado satisfactoriamente')
        # print('\n')       
        
        # elapsed_time = pd.Timestamp('now') - start_time
        # print('Tiempo de procesamiento fue de ',elapsed_time)   
        
        return afectacion_saldo
    
    
    
    
    ''' SECCIÓN 8 '''
    
    def afectacion_historica_estimada(df_to_input, df_saldo_anual_sr_input, usura_mes_actualizacion,  usura_mes_siguiente_actualizacion):  
        # start_time = pd.Timestamp('now') 
    
        saldo_k = []
        periodo = []
        
        # lectura de saldo capital para cada mes
        df_saldo_anual_sr = df_saldo_anual_sr_input
        df_mes = df_saldo_anual_sr[(df_saldo_anual_sr['tipo_tasa'] == 'Tasa actual')]
        saldo_capital = df_mes.saldo_actual_trasaccion.sum()
        mes = df_mes.mes.unique()
        mes = mes[0]
        saldo_k.append(saldo_capital)
        periodo.append(mes)
               
        saldos = pd.DataFrame({"periodo":periodo,"saldo_capital":saldo_k}) 
    
        # Calcular saldo expuesto       
        saldo_expuesto = []
        impacto_pyg = []
        tipo_variacion = []
        
        columnas = ['saldo_actual_trasaccion','tasa_interes','TASA_EA','periodo']
        df_mes = df_to_input[columnas]
        df_mes = df_mes.rename(columns={"TASA_EA":"tasa_ea_original"})
        df_mes['interes_ea'] = (((((df_mes['tasa_interes']/100)+1)**12)-1)*100).round(2)
        periodo = df_mes.periodo.unique()
        periodo = periodo[0] 
            
        df_mes = df_mes[['saldo_actual_trasaccion','tasa_ea_original','interes_ea']]                
        
        variacion_usura = []
        anterior = usura_mes_actualizacion
        actual = usura_mes_siguiente_actualizacion
        variacion = actual - anterior
        variacion_usura.append(variacion)
        df_usura = pd.DataFrame({"tasa_usura":usura_mes_actualizacion,
                                 "variacion_usura":variacion_usura})
        
        tasa_usura_mes = usura_mes_actualizacion
        variacion_usura_mes = variacion
        # variacion_usura_mes = round(variacion_usura_mes,2)
            
        # si la usura baja:
        if variacion_usura_mes < 0:
                
            minimo = tasa_usura_mes - abs(variacion_usura_mes)
                
            # Calcula el saldo expuesto y el impacto a la subida
            df_exp_bajada_usura = df_mes[df_mes['interes_ea'] >= minimo]
            df_exp_bajada_usura = df_exp_bajada_usura.reset_index(drop=True)
            saldo_exp_bajada_usura = df_exp_bajada_usura.saldo_actual_trasaccion.sum()
            impacto_pyg_bajadaNpbs = (saldo_exp_bajada_usura * (abs(variacion_usura_mes)/100)) * (-1)
                
            saldo_expuesto.append(round(saldo_exp_bajada_usura, 2))
            impacto_pyg.append(round(impacto_pyg_bajadaNpbs, 2))
            var_str = round(variacion_usura_mes * 100)
            tipo_variacion.append(f'Variacion: {var_str} Pbs') 
            
        # Si la usura sube:                
        if variacion_usura_mes > 0:
                
            usura_final = tasa_usura_mes + abs(variacion_usura_mes)
            df_exp_subida_usura = df_mes[df_mes['tasa_ea_original'] > tasa_usura_mes]
            df_exp_subida_usura =df_exp_subida_usura.reset_index(drop=True)
                
            tasa_final = []
            for tasa_original in df_exp_subida_usura['tasa_ea_original']:
                if tasa_original <= usura_final:
                    tasa_final.append(tasa_original)
                elif usura_final < tasa_original:
                    tasa_final.append(usura_final)
                        
            df_tasa_final = pd.DataFrame({"tasa_final":tasa_final})
            diferencia_de_tasas  = tasa_final - df_exp_subida_usura['interes_ea']
            diferencia_de_tasas = pd.DataFrame({"diferencia_tasas": (df_tasa_final['tasa_final'] - df_exp_subida_usura['interes_ea']) / 100})
                
            df_calculo_impacto = pd.concat([df_exp_subida_usura,diferencia_de_tasas], axis='columns')
            df_calculo_impacto['impacto_variacion_Npbs'] = df_calculo_impacto['saldo_actual_trasaccion'] * df_calculo_impacto['diferencia_tasas']
                
            impacto_pyg_subidaNpbs_usura = df_calculo_impacto.impacto_variacion_Npbs.sum()
            saldo_exp_subidaNpbs_usura = df_exp_subida_usura.saldo_actual_trasaccion.sum()
                
            saldo_expuesto.append(round(saldo_exp_subidaNpbs_usura))
            impacto_pyg.append(round(impacto_pyg_subidaNpbs_usura))
            var_str = round(variacion_usura_mes * 100)
            tipo_variacion.append(f'Variacion: {var_str} Pbs')
                
        if variacion_usura_mes == 0:
                
            saldo_capital_exp_sinVariacion = 0
            impacto_pyg_sinVariacion = 0
            saldo_expuesto.append(saldo_capital_exp_sinVariacion)
            impacto_pyg.append(impacto_pyg_sinVariacion)
            var_str = round(variacion_usura_mes * 100)
            tipo_variacion.append(f'Variacion: {var_str} Pbs')

        saldoExp_impactoPyg = pd.DataFrame({"saldo_expuesto":saldo_expuesto,
                                            "impacto_pyg":impacto_pyg,
                                            "tipo_variacion":tipo_variacion})
        
        # Unir saldos de capital, tasas y sus variaciones, saldos expuesto e impacto pyg

        saldos_tasas = pd.concat([saldos, df_usura, saldoExp_impactoPyg], axis='columns')
        saldos_tasas = saldos_tasas[['periodo','saldo_capital','tasa_usura','variacion_usura','saldo_expuesto','impacto_pyg','tipo_variacion']]
         
        saldos_tasas.variacion_usura = saldos_tasas.variacion_usura.round(2)
        saldos_tasas["saldo_capital"] = saldos_tasas["saldo_capital"].astype('str')
        saldos_tasas["tasa_usura"] = saldos_tasas["tasa_usura"].astype('str')
        saldos_tasas["variacion_usura"] = saldos_tasas["variacion_usura"].astype('str')
        saldos_tasas["saldo_expuesto"] = saldos_tasas["saldo_expuesto"].astype('str')
        saldos_tasas["impacto_pyg"] = saldos_tasas["impacto_pyg"].astype('str')
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        for index, row in saldos_tasas.iterrows():
            query_insert = """INSERT INTO afectacion_historica_estimada 
                            ("periodo","saldo_capital","tasa_usura","variacion_usura",
                             "saldo_expuesto","impacto_pyg","tipo_variacion") 
                            VALUES (?,?,?,?,?,?,?) 
                                       """
            cursor.execute(query_insert, [row["periodo"], row["saldo_capital"],
                                          row["tasa_usura"], row["variacion_usura"],
                                           row["saldo_expuesto"], row["impacto_pyg"],
                                            row["tipo_variacion"]])
    
        con.commit()
        cursor.close()
        # print('Procesamiento: afectación histórica estimada completada satisfactoriamente')
        # print('\n')             
        
        # elapsed_time = pd.Timestamp('now') - start_time
        # print(f"Tiempo de procesamiento: {elapsed_time}")
        # print('* '*25)
        return saldos_tasas
        
        
    
    ''' SECCIÓN 9 '''
    def estimacion_sensibilidad(df_input, tasa_usura_input):
        # start_time = pd.Timestamp('now') 

        tasas_usura = tasa_usura_input
    
        variaciones_usura = [-1,-0.6,-0.3,0,0.3,0.6,1]
        
        saldo_expuesto_completo = []
        impacto_pyg_completo = []
        variacion_usura_completo = []
        periodo = []
    
        columnas = ['saldo_actual_trasaccion','tasa_interes','TASA_EA','periodo']
        df_mes = df_input[columnas]
        df_mes = df_mes.rename(columns={"TASA_EA":"tasa_ea_original"})
        df_mes['interes_ea'] = (((((df_mes['tasa_interes']/100)+1)**12)-1)*100).round(2)
        mes_periodo = df_mes.periodo.unique()
        mes_periodo = mes_periodo[0] 
            
        df_mes = df_mes[['saldo_actual_trasaccion','tasa_ea_original','interes_ea']]                
            
        tasa_usura_mes = tasas_usura
                                   
        for variacion in variaciones_usura:
            
            if variacion < 0:
                minimo = tasa_usura_mes - abs(variacion)
                
                df_exp_bajada_usura = df_mes[df_mes['interes_ea'] >= minimo]
                df_exp_bajada_usura = df_exp_bajada_usura.reset_index(drop=True)
                saldo_exp_bajada_usura = df_exp_bajada_usura.saldo_actual_trasaccion.sum()
                impacto_pyg_bajadaNpbs = (saldo_exp_bajada_usura * (abs(variacion)/100)) * (-1)
                saldo_expuesto_completo.append(saldo_exp_bajada_usura)
                impacto_pyg_completo.append(impacto_pyg_bajadaNpbs)
                variacion_usura_completo.append(variacion)
            
            if variacion > 0:
                
                usura_final = tasa_usura_mes + abs(variacion)
                df_exp_subida_usura = df_mes[df_mes['tasa_ea_original'] > tasa_usura_mes]
                df_exp_subida_usura =df_exp_subida_usura.reset_index(drop=True)
                
                tasa_final = []
                for tasa_original in df_exp_subida_usura['tasa_ea_original']:
                    if tasa_original <= usura_final:
                        tasa_final.append(tasa_original)
                    elif usura_final < tasa_original:
                        tasa_final.append(usura_final)
                        
                df_tasa_final = pd.DataFrame({"tasa_final":tasa_final})
                diferencia_de_tasas  = tasa_final - df_exp_subida_usura['interes_ea']
                diferencia_de_tasas = pd.DataFrame({"diferencia_tasas": (df_tasa_final['tasa_final'] - df_exp_subida_usura['interes_ea']) / 100})
                
                df_calculo_impacto = pd.concat([df_exp_subida_usura,diferencia_de_tasas], axis='columns')
                df_calculo_impacto['impacto_variacion_Npbs'] = df_calculo_impacto['saldo_actual_trasaccion'] * df_calculo_impacto['diferencia_tasas']
                
                impacto_pyg_subidaNpbs_usura = df_calculo_impacto.impacto_variacion_Npbs.sum()
                saldo_exp_subidaNpbs_usura = df_exp_subida_usura.saldo_actual_trasaccion.sum()
                
                saldo_expuesto_completo.append(saldo_exp_subidaNpbs_usura)
                impacto_pyg_completo.append(impacto_pyg_subidaNpbs_usura)
                variacion_usura_completo.append(variacion)
                
            if variacion == 0:
                saldo_exp = 0     
                impacto_pyg = 0
                saldo_expuesto_completo.append(saldo_exp)
                impacto_pyg_completo.append(impacto_pyg)
                variacion_usura_completo.append(variacion)
            
            periodo.append(mes_periodo)
        
        df_proyeccion = pd.DataFrame({"Variacion Usura":variacion_usura_completo,"Saldo expuesto":saldo_expuesto_completo, "Impacto PyG":impacto_pyg_completo, "Periodo":periodo})    

        df_proyeccion["Variacion Usura"] = df_proyeccion["Variacion Usura"].astype('str')
        df_proyeccion["Saldo expuesto"] = df_proyeccion["Saldo expuesto"].astype('str')
        df_proyeccion["Impacto PyG"] = df_proyeccion["Impacto PyG"].astype('str')
        df_proyeccion["Periodo"] = df_proyeccion["Periodo"].astype('str')
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        for index, row in df_proyeccion.iterrows():
            query_insert = """INSERT INTO estimacion_sensibilidad 
                            ("Variacion Usura","Saldo expuesto","Impacto PyG",
                             "Periodo") 
                            VALUES (?,?,?,?) """
            cursor.execute(query_insert, [row["Variacion Usura"], row["Saldo expuesto"],
                                          row["Impacto PyG"], row["Periodo"]])
    
        con.commit()
        cursor.close()
        # print('Procesamiento: afectación histórica estimada completada satisfactoriamente')
        # print('\n')         

        # elapsed_time = pd.Timestamp('now') - start_time
        # print(f"Tiempo de procesamiento: {elapsed_time}")
        
        return df_proyeccion 
    
    
    
    
    ''' SECCIÓN 10 '''
    def actualizar_escenarios_usura(periodo, maximo, percentil_95, promedio, percentil_25, minimo):
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        query_insert = """INSERT INTO Escenarios_Usura_EaR 
                            ("periodo","Mínimo","Percentil 25",
                             "Percentil 95", "Máximo","Promedio") 
                            VALUES (?,?,?,?,?,?) """
        cursor.execute(query_insert, [periodo, minimo,percentil_25,percentil_95,
                                      maximo, promedio])
    
        con.commit()
        cursor.close()
        # print(f'Actualizado el periodo {periodo} satisfactoriamente')
        
    
    
    
    ''' SECCIÓN 11 '''
    
    def afectacion_ingresos_conTasaMv(df_input ,usura_input_corte, usura_input_mes_nuevo):
        
        # afectacion_variableTasaMv
        # start_time = pd.Timestamp('now') 

        tasas_usura = [usura_input_corte, usura_input_mes_nuevo]
        df_usura = pd.DataFrame({"tasa_usura_ea":tasas_usura})
        df_usura['tasa_usura_ea'] = df_usura['tasa_usura_ea'] / 100
        df_usura['tasa_usura_mv'] = df_usura['tasa_usura_ea'].apply(lambda x: round((((1+x)**(30/360))-1)*100,2))
        df_usura['tasa_usura_ea'] = df_usura['tasa_usura_ea'] * 100
        
        
        variacion_usura = []
        anterior = float(df_usura.iloc[0][1])
        actual = float(df_usura.iloc[1][1])
        variacion = round(actual - anterior,2)
        variacion_usura.append(variacion)
        df_usura = df_usura[['tasa_usura_ea','tasa_usura_mv']]
        
        df_usura = df_usura.iloc[:-1,:]
        df_usura["variacion_usura_mv"] = variacion_usura
      
        impacto_pyg_estimado_tasaVar = []
    
        periodo_var = []
    
        columnas = ['saldo_actual_trasaccion','tasa_interes','periodo']
        df_mes_var = df_input[columnas]
        df_mes_var['tasa_interes'] = df_mes_var['tasa_interes'] /100
        df_mes_var['impacto_pyg'] = df_mes_var.tasa_interes * df_mes_var.saldo_actual_trasaccion
        impacto_var = df_mes_var.impacto_pyg.sum()
        mes_periodo_var = df_mes_var.periodo.unique()
        mes_periodo_var = mes_periodo_var[0] 
        
        impacto_pyg_estimado_tasaVar.append(impacto_var)
        periodo_var.append(mes_periodo_var)
        
        # print('Ejecutando calculo de ingresos con tasa M.V.')
        
        df_afectacion_estimada_tasaCorte = pd.DataFrame({"periodo":periodo_var,"impacto_pyg":impacto_pyg_estimado_tasaVar})
        # df_afectacion_estimada_tasaCorte = pd.concat([df_afectacion_estimada_tasaCorte,df_usura], axis='columns')
        df_afectacion_estimada_tasaCorte['Impacto'] = 'Ingresos Tasa actual'
        
        # afectacion_constanteTasaMv
    
        impacto_pyg_estimado_cnste = []
    
        periodo_cnste = []
    
        columnas = ['saldo_actual_trasaccion','tasa_interes','TASA_EA','periodo']
        df_mes_cnst = df_input[columnas]
        df_mes_cnst = df_mes_cnst.rename(columns={"TASA_EA":"tasa_ea_original","tasa_interes":"tasa_interes_mv"})
        df_mes_cnst['tasa_ea_original'] = df_mes_cnst['tasa_ea_original'] /100
        df_mes_cnst['tasa_original_mv'] = df_mes_cnst['tasa_ea_original'].apply(lambda x: round((((1+x)**(30/360))-1)*100,2))
        df_mes_cnst['tasa_original_mv'] = df_mes_cnst['tasa_original_mv'] /100
        df_mes_cnst['impacto_pyg'] = df_mes_cnst.tasa_original_mv * df_mes_cnst.saldo_actual_trasaccion
        df_mes_cnst['tasa_original_mv'] = df_mes_cnst['tasa_original_mv'] *100
        impacto_cnst = df_mes_cnst.impacto_pyg.sum()
        mes_periodo_cnst = df_mes_cnst.periodo.unique()
        mes_periodo_cnst = mes_periodo_cnst[0] 
        
        impacto_pyg_estimado_cnste.append(impacto_cnst)
        periodo_cnste.append(mes_periodo_cnst)
        # print(f'Calculo de impacto PyG con tasa original M.V para {mes_periodo_cnst} completado satisfactoriamente')
        # print('\n ')
        # print('Ejecutando...')  
        
        df_afectacion_estimada_tasaOriginal = pd.DataFrame({"periodo":periodo_cnste,"impacto_pyg":impacto_pyg_estimado_cnste})
        # df_afectacion_estimada_tasaOriginal = pd.concat([df_afectacion_estimada_tasaOriginal,df_usura], axis='columns')
        df_afectacion_estimada_tasaOriginal["Impacto"] = 'Ingresos Tasa original'
    
        ingresos_estimados_porTasa = pd.concat([df_afectacion_estimada_tasaCorte,df_afectacion_estimada_tasaOriginal])
        
        ingresos_estimados_porTasa.impacto_pyg = ingresos_estimados_porTasa.impacto_pyg.astype('str')    
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()
        
        for index, row in ingresos_estimados_porTasa.iterrows():
            query_insert = """INSERT INTO ingresos_estimados_tasaMV 
                                ("periodo","impacto_pyg","Impacto") 
                                VALUES (?,?,?) """
            cursor.execute(query_insert, [row["periodo"], row["impacto_pyg"],row["Impacto"]])

        con.commit()
        cursor.close()
        
        
        diferencia_impacto = pd.DataFrame()
        diferencia_impacto['Impacto en Ingresos'] = [impacto_cnst - impacto_var]
        diferencia_impacto['periodo'] = [mes_periodo_cnst]
        diferencia_impacto['tasa_usura_mv'] = [anterior]
        
        diferencia_impacto['Impacto en Ingresos'] = diferencia_impacto['Impacto en Ingresos'].astype('str')
        diferencia_impacto['tasa_usura_mv'] = diferencia_impacto['tasa_usura_mv'].astype('str')
        
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()
        
        for index, row in diferencia_impacto.iterrows():
            query_insert = """INSERT INTO diferencia_ingresos_tasaMV 
                                ("Impacto en Ingresos","periodo","tasa_usura_mv") 
                                VALUES (?,?,?) """
            cursor.execute(query_insert, [row["Impacto en Ingresos"], row["periodo"],row["tasa_usura_mv"]])

        con.commit()
        cursor.close()
        # print('\n ')
        # print(f'Calculos con tasa M.V. del periodo {mes_periodo_cnst} completados satisfactoriamente')        
        # elapsed_time = pd.Timestamp('now') - start_time
        # print(f"Tiempo de procesamiento: {elapsed_time}")
        
        return ingresos_estimados_porTasa, diferencia_impacto
    
    

    
    ''' SECCIÓN 12 '''
    
    def saldo_usura(tasa_usura, df_mes_input):
    
        # start_time  = pd.Timestamp('now')

        
        saldo_expuesto = []
        impacto_pyg = []
        tipo_variacion = []
        rango = []
        
        columnas = ['saldo_actual_trasaccion','tasa_interes','TASA_EA','periodo']
        df = df_mes_input[columnas]
        df = df.rename(columns={"TASA_EA":"tasa_ea_original"})
        df['interes_ea'] = (((((df['tasa_interes']/100)+1)**12)-1)*100).round(2)
        periodo = df.periodo.unique()
        periodo = periodo[0]
        
        df_mes = df[['saldo_actual_trasaccion','tasa_ea_original','interes_ea']]
        
        tasa_usura = tasa_usura
        
        # Usura aumenta 450 pbs
        
        usura_final = tasa_usura + 4.5
        df_exp_mas450pbs_usura = df_mes[df_mes['tasa_ea_original'] > tasa_usura]
        df_exp_mas450pbs_usura = df_exp_mas450pbs_usura.reset_index(drop=True)
        
        tasa_final = []
        for tasa_original in df_exp_mas450pbs_usura['tasa_ea_original']:
            if tasa_original <= usura_final:
                tasa_final.append(tasa_original)
            elif usura_final < tasa_original:
                tasa_final.append(usura_final)
                
        df_tasa_final = pd.DataFrame({"tasa_final":tasa_final})
        
        diferencia_de_tasas  = tasa_final - df_exp_mas450pbs_usura['interes_ea']
        
        diferencia_de_tasas = pd.DataFrame({"diferencia_tasas": (df_tasa_final['tasa_final'] - df_exp_mas450pbs_usura['interes_ea']) / 100})
        
        df_calculo_impacto = pd.concat([df_exp_mas450pbs_usura,diferencia_de_tasas], axis='columns')
        df_calculo_impacto['impacto_variacion_Npbs'] = df_calculo_impacto['saldo_actual_trasaccion'] * df_calculo_impacto['diferencia_tasas']
        
        impacto_pyg_mas450pbs = df_calculo_impacto.impacto_variacion_Npbs.sum()
        saldo_exp_mas450pbs_usura = df_exp_mas450pbs_usura.saldo_actual_trasaccion.sum()
        
        saldo_expuesto.append(round(saldo_exp_mas450pbs_usura))
        impacto_pyg.append(round(impacto_pyg_mas450pbs))
        tipo_variacion.append('sube 450 Pbs')
        rango.append(periodo)
        
        # Usura disminuye 450 pbs
        
        usura_final_bajada = tasa_usura - 4.5
        
        df_exp_menos450pbs_usura = df_mes[df_mes['interes_ea'] >= usura_final_bajada]
        df_exp_menos450pbs_usura = df_exp_menos450pbs_usura.reset_index(drop=True)
        saldo_exp_menos450pbs_usura = df_exp_menos450pbs_usura.saldo_actual_trasaccion.sum()
        impacto_pyg_menos450pbs = (saldo_exp_menos450pbs_usura * 0.045) * (-1)
        
        saldo_expuesto.append(round(saldo_exp_menos450pbs_usura, 2))
        impacto_pyg.append(round(impacto_pyg_menos450pbs,2))
        tipo_variacion.append('baja 450 Pbs')
        rango.append(periodo)
           
        afectacion_saldo = pd.DataFrame({"periodo":tipo_variacion})
        afectacion_saldo["periodo"] = rango
        afectacion_saldo["saldo_expuesto"] = saldo_expuesto
        afectacion_saldo["impacto_pyg"] = impacto_pyg
        afectacion_saldo["tipo_variacion"] = tipo_variacion
        
        afectacion_saldo["saldo_expuesto"] = afectacion_saldo["saldo_expuesto"].astype('str')
        afectacion_saldo["impacto_pyg"] = afectacion_saldo["impacto_pyg"].astype('str')
        
        import pyodbc as sql   
        '''Crear conexión con repositorio SQL insertar datos'''
        con = sql.connect(
                            "DRIVER={SQL Server Native Client 11.0};"
                            "SERVER=SADGVSQL2K19U\DREP,57201;"
                            "Database=SieT;"
                            "Trusted_Connection=yes;")            

        cursor = con.cursor()

        for index, row in afectacion_saldo.iterrows():
            query_insert = """INSERT INTO saldo_usura 
                            ("periodo","Saldo expuesto","Impacto PyG","Tipo variacion") 
                            VALUES (?,?, ?, ?) 
                                        """
            cursor.execute(query_insert, [row["periodo"], row["saldo_expuesto"],
                                          row["impacto_pyg"], row["tipo_variacion"]])
    
        con.commit()
        cursor.close()
        # print('Procesamiento: Saldo usura completado satisfactoriamente')        
        # elapsed_time = pd.Timestamp('now') - start_time
        # print('Tiempo de procesamiento fue de ',elapsed_time)   
    
        return afectacion_saldo
    

    def ejecucion_procesamiento_tc(archivo_composicion_saldo,archivo_fact_txc,
                                   nombre_mes,mes_actualizacion, anio_actualizacion,
                                   tasa_usura_mesActualizacion,tasa_implicita,
                                   tasa_usura_mesSiguiente):
        start_time = pd.Timestamp('now')
        print('Iniciando proceso de actualización de datos')
        print('Este proceso puede durar aproximadamente 3 horas y 45 minutos, una vez finalice haga clic en actualizar en el dashboard de Power BI')
        print('\n ')
        print('Calculando composición de saldo...')
        print('\n ')
        procesamiento_tc.composicion_saldo(archivo_composicion_saldo)
        print('\n ')
        print('Composición de saldo calculada satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print(f'Realizando pre procesamiento de datos a {archivo_fact_txc} ...')
        print('NOTA: Este proceso puede tomar la mayoría del tiempo del procesamiento')
        procesamiento_tc.procesar(archivo_fact_txc, nombre_mes)
        print('\n ')
        print('Pre procesamiento realizado satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Obteniendo tasas originales...')
        df_to = procesamiento_tc.tasas_originales(nombre_mes,mes_actualizacion,anio_actualizacion)
        print('\n ')
        print('Tasas originales obetenidas satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Realizando segmentantación del saldo...')
        procesamiento_tc.segmentacion_saldo(df_to)
        print('\n ')
        print('Segmentación del saldo completado satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Calculando tasa facial, cargando tasa usura e implicita...')        
        procesamiento_tc.tasas_usura_implicita_facial(df_to, tasa_usura_mesActualizacion, tasa_implicita)
        print('\n ')
        print('Cálculo y cargue completado satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Segmentando el saldo en rangos de tasas originales y de corte...')        
        saldo_sinRango = procesamiento_tc.saldo_to_mes(df_to)
        print('\n ')
        print('Segmentación completada satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Calculando afectacion saldo ante subidas o bajadas de la usura en 100 pbs...')        
        procesamiento_tc.afectacion_100pbs(tasa_usura_mesActualizacion, df_to)
        print('\n ')
        print('Cálculo finalizado satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Calculando afectación histórica estimada por cambios de la usura...')        
        procesamiento_tc.afectacion_historica_estimada(df_to, saldo_sinRango, tasa_usura_mesActualizacion,tasa_usura_mesSiguiente)
        print('\n ')
        print('Cálculo finalizado satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Calculando estimación sensibilidad...')        
        procesamiento_tc.estimacion_sensibilidad(df_to, tasa_usura_mesActualizacion)
        print('\n ')
        print('Cálculo finalizado satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Calculando afectación histórica a ingresos con tasa M.V. ...')        
        procesamiento_tc.afectacion_ingresos_conTasaMv(df_to, tasa_usura_mesActualizacion, tasa_usura_mesSiguiente)
        print('\n ')
        print('Cálculo finalizado satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')
        print('Calculando saldo usura...')        
        procesamiento_tc.saldo_usura(tasa_usura_mesActualizacion, df_to)
        print('\n ')
        print('Cálculo finalizado satisfactoriamente')
        print('\n ')
        print('* '*25)
        print('\n ')        
        elapsed_time = pd.Timestamp('now') - start_time
        print('Tiempo de procesamiento tomado para actualizar el dashboard fue de ',elapsed_time)
        print('* '*25)
        print('\n ')
        print(f'Proceso finalizado para el mes {mes_actualizacion} y año {anio_actualizacion}, por favor haga clic en el botón actualizar del tablero de control Dashboard TC de Power BI')
        

    



    
        


    

