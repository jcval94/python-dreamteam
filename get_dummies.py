 '''Función que a partir de un dataframe y un listado de variables, nos regresa 
    dichas variables categoricas en una columna con un indice ponderado
'''
def get_dummies(df, list_var):
    from pyspark.ml.feature import StringIndexer
    for x in list_var:    
        indexer = StringIndexer(inputCol = str(x), outputCol=str(x) + "_Index")
        df = indexer.fit(df).transform(df)
    return df
