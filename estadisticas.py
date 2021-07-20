def estadisticas(listado_, data_):
    '''
    Función que  a partir de una lista de variables y un dataframe, nos regresa los estadisticos:
    -percentil 25
    -percentil 50 (mediana)
    -percentil 75
    -media
    '''
    n = 0
    # Percentil 25
    for y in listado_:
        
        #Percentil  25
        grp_window = Window.partitionBy('customer_id')
        magic_percentile = F.expr('percentile_approx(' + str(y) +', 0.25)')
        data_ = data_.withColumn('per25_' + str(y), magic_percentile.over(grp_window))
        
        # Mediana
        grp_window = Window.partitionBy('customer_id')
        magic_percentile = F.expr('percentile_approx(' + str(y) +', 0.5)')
        data_ = data_.withColumn('med_' + str(y), magic_percentile.over(grp_window))
        
        #Percentil 75
        grp_window = Window.partitionBy('customer_id')
        magic_percentile = F.expr('percentile_approx(' + str(y) +', 0.75)')
        data_ = data_.withColumn('per75_' + str(y), magic_percentile.over(grp_window))
        
        #Media
        data_2 = data_.groupBy('customer_id', 'claim_register1_date')\
        .agg(F.mean(F.col((str(y))))).alias('media' +  str(y))
        data_ = data_2.join(data_, how='left', on=['customer_id', 'claim_register1_date'])        
    return data_