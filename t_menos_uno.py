'''
Función que  a partir de una lista de variables y un dataframe, nos regresa esas variables 
en un T-1
'''
def t_menos_uno(variables_, df_, lag_):
    # Creamos para la variable Monto su respectivo T-1
    for x in variables_:
        df_ = df_.select("*",\
        F.lag(x).over(Window.partitionBy('customer_id')\
                        .orderBy('customer_id','claim_register1_date')).alias(str(x) + '_' + str(lag_)))
    return df_