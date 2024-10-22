from google.cloud import bigquery

class BigQueryTable:
  def __init__(self, project_id:str = 'poner el nombre del proyecto', datased_id:str = 'poner Nombre del dataset'):
    self.project_id = project_id
    self.datased_id = datased_id
    self.client = bigquery.Client(project=project_id)

def create_metada_table(self, table_id:str = 'poner nombre de la tabla'):
  schema = [
    bigquery.SchemaField("Nombre primera columna", "STRING"), #Cambiar STRING segun corresponda 
    bigquery.SchemaField("Nombre segunda columna", "STRING"),
    bigquery.SchemaField("Nombre tercera columna", "STRING"), #seguir agregando columnas segun corresponda
  ]

  table_ref = self.client.dataset(self.dataset_id).table(table_id) #se define la referencia de la tabla
  table = bigquery.Table(table_ref, schema=schema)
  table = self.client.create_table(table, exists_ok=True) #se crea table si es que no existe para evitar duplicidad


def delete_table(self, table_id:str):
  table_ref = self.client.dataset(self.dataset_id).table(table_id) #se define la referencia de la tabla
  self.client.delete_table(table_ref, nor_found_of=True)

def query_table(self, table_name:str):
  query = f"SELECT * FROM '{self.project_id}.{self.dataset_id}.{table_name}' LIMIT 1000"
  query_job = self.client.query(query)
  return query_job.to_dataframe()
