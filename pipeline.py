import pandas as pd
import yaml
import json
import argparse

from utils import list_blobs, gemini_request
from bq_tables import BigQueryTable
import pandas_gbq
from prompts import sentiment_prompt_markdown

class InsightsPipeline:

  def __init__(self,
               project_id:str = "poner nombre del proyecto",
               mp3_bucket:str = "poner nombre del bucket",
               mp3_folder:str = "carpeta en la que se encuentra del bucket"):
        
        self.project_id = project_id
        self.mp3_bucket = mp3_bucket
        slef.mp3_folder = mp3_folder

def audio_metada(self,
                 dataset_id:str = "nombre que esta en bigquery"
                 bq_table_name:str = "nombre de la tabla de bigquery"):
      df = pd.DataFrame()
      mp3_files_list = list_blobs(self.project_id, self.mp3_bucket, self.mp3_folder, ".mp3")
      df["uri"] = mp3_files_list

      file_name_list = [mp3.split("/")[-1] for mp3 in mp3_files_list]
      df["file_name"] = file_name_list

      empresa_list = [file_name.split("_")[0] for file_name in file_name_list]
      df["empresa"] = empresa_list

      internal_identificadores = ["2"]
      internal = df[df["empresas"].isin(internal_identificadores)].copy()
      external = df[~df["empresas"].isin(internal_identificadores)].copy()

      del df
      
      internal["fecha"] = internal["file_name"].str.split("_").str[1]
      internal["hora"] = internal["file_name"].str.split("_").str[2]
      internal["numero_origen"] = internal["file_name"].str.split("_").str[3]
      internal["numero_destino"] = internal["file_name"].str.split("_").str[4]
      internal["identificador_unico"] = internal["file_name"].str.split("_").str[5].str.split(".").str[0]
      
      
      external["fecha"] = external["file_name"].str.split("_").str[1]
      external["hora"] = external["file_name"].str.split("_").str[2]
      external["numero_origen"] = "no disponible"
      external["numero_destino"] = external["file_name"].str.split("_").str[3]
      external["identificador_unico"] = external["file_name"].str.split("_").str[4].str.split(".").str[0]


      df_metada = pd.concat([internal, external])

      del internal, external


      bqt = BugQueryTable(project_id=self.project_id, dataset_id=dataset_id)
      bqt.create_metadata_table(bq_table_name)

      pandas_gbq.to_gbq(df_metadata, destination_table=f"{dataset_id}.{bq_table_name}", project_id=self.project_id,
                        if_exist="append")
      return df_metadata

  def audio_transcription(self,
                          dataset_id:str = "agregar dataset de bigquery",
                          bq_table_name:str = "agregar tabla de bigquery"):

      df_transcription = pd.DataFrame()

      mp3_files_list = list_blobs(self.project_id, self.mp3_bucket, self.mp3_folder, ".mp3")

      with open('prompts.yml', 'r') as file:
        config = yaml.safe_load(file)

      transcription_lis= []
      raw_transcription_list = []
      processed_files_list = []

      self.files_got_error = []
      for uri in mp3_files_list:
        print(f"Processing: {uri}")
          try:
            raw_transcription = gemini_request(system_inst=config["system_introduction"], prompt=config["prompt"], audio_uri=uri)
            raw_transcription_list.append(raw_transcription)
            processed_files_list.append(uri)

            try:
              cleaned_json = raw_transcription.split('```json',1)[1].split(('```',1)[0].strip()
              transcription_list.append(cleaned_json)
           except (IndexError, json.JSONDecodeError) as e:
              print(f"Error parsing transcription for {uri}: {e}")

          except Exception as e:
            print("Could not create transcription")
            self.files_got_error.append(uri)

      df_transcription["uri"] = processed_files_list
      df_transcription["transcription_raw"] = raw_transcription_list
      df_transcription["transcription_cleaned"] = transcription_list

      bqt = BigQueryTable(project_id=self.project_id, dataset_id=dataset_id)
      bqt.create_transcription_table(bq_table_name)

      pandas_gbq.to_gbq(df_transcription, destination_table=f"{dataset_id}.{bq_table_name}", project_id=self.project_id, if_exists="append")

      return df_transcription


  def sentiment_categories_with_reasoning(self,
                                          dataset_id:str = "dataset de bigquery",
                                          bq_table_name:str = "tabla de bigquery"):

  df_insights = self.df_transcription.copy()
  insights_list = []
  sentimiento_list = []
  categorizacion_list = []
  descripcion_list = []
  razonamiento_list = []

  for index, row in df_insights.interrows():
    system_inst, prompt = sentiment_prompt_markdown(row["transcription_raw"])
    insights = gemini_request(system_inst, prompt=prompt, audio_uri=None)

    cleaned_json = insights.split('```json', 1)[1].split('```', 1)[0].strip()
    insights_json = json.loads(cleaned_json)
    sentimiento_list.append(insights_json["sentimiento"])
    categorizacion_list.append(insights_json["categorizacion"])
    descripcion_list.append(insights_json["descripcion"])
    razonamiento_list.append(insights_json["razonamiento"])

  df_insights["insights"] = insights_list
  df_insights["sentimiento"] = sentimiento_list
  df_insights["categorizacion"] = categorizacion_list
  df_insights["descripcion"] = descripcion_list
  df_insights["razonamiento"] = razonamiento_list
  
  bqt = BigQueryTable(project_id=self.project_id, dataset_id=dataset_id)
  bqt.create_insights_table(bq_table_name)

  df_insights = df_insights.drop(columns=["transcription_raw", "transcription_cleaned"])

  pandas_gbq.to_gbq(df_insights, destination_table=f"{dataset_id}.{bq_table_name}", project_id=self.project_id, if_exists="append")

  return df_insights

def pipeline(self):
  self.df_metadata = self.audio_metadata(dataset_id="aqui va el dataset bigquery",
                                         bq_table_name="la tabla del dataset")
  self.df_transcription = self.audio_transcription(dataset_id="aqui va el dataset bigquery",
                                         bq_table_name="la tabla del dataset")
  self.df_insights = self.sentiment_categories_with_reasoning(dataset_id="aqui va el dataset bigquery",
                                         bq_table_name="la tabla del dataset")

if __name__ == "__main__":
  parser = argparse.ArgumentParser(desciption="Run InsightsPipeline with optional arguments")
  parser.add_argument("--mp3_bucket", type=str, default="aqui va el nombre del bucket", help="MP3 bucket name")
  parser.add_argument("--mp3_folder", type=str, default="aqui la carpeta del bucket", help="MP3 folder path")

  args = parser.parse_args()
  ip = InsightsPipeline(project_id="nombre del projecto",
                        mp3_bucket=args.mp3_bucket,
                        mp3_folder=args.mp3_folder)
  ip.pipeline()
