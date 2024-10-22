#instrucciones para audios
from google.cloud import storage
import librosa
import soundfile as sf
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from vertexai.generative_models import HarmCategory, HarmBlockThreshold, SafetySetting


def gemini_request(system_inst:str,
                   prompt:str,
                   audio_uri:str,
                   project:str = "poner nombre del proyecto",
                   location:str = "depende de cual este habilitada pero la mas razonable debiese ser us-east4",
                   model_name:str = "gemini-1.5-flash-002" # Existen mas pero para este ejemplo se deja este
                   ):
  vertexai.init(project=project, location=location)
  model = GenerativeModel(
    model_name=model_name,
    system_instruction=[system_inst] #aqui dejar si es que necesita mayor contexto del audio
    )
  generation_config = {
    "max_output_tokens": 8192,
    "temperature": 0, #se deja para menor creatividad de las respuestas.
    "top_p": 1,
    "top_k": 1,
    "seed": 42
    }
  safety_settings = [
    SafetySetting(
      category=HarmCategory.HARM_CATEGORY_HATE_SPEECH,
      threshold=HarmBlockThreshold.BLOCK_NONE,
      ),
    SafetySetting(
      category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
      threshold=HarmBlockThreshold.BLOCK_NONE,
      ),
    SafetySetting(
      category=HarmCategory.HARM_CATEGORY_HARASSMENT,
      threshold=HarmBlockThreshold.BLOCK_NONE,
      ),
    SafetySetting(
      category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
      threshold=HarmBlockThreshold.BLOCK_NONE,
      ),
    ]
  if audio_uri:
    audio_file_uri = audio_uri
    audio_file = Part.from_uri(audio_file_uri, mime_type="audio/mpeg")
    content = [audio_file, prompt]
  else:
    content = [prompt]
  response = model.generate_content(
    content,
    generation_config=generation_config,
    safety_settings=safety_settings,
    stream=False,
    )
  return response.text

def list_blobs(project_id, bucket_name, folder_prefix, endswith):
  client = storage.Client(project_id=project_id)
  bucket_name = bucket_name
  folder_prefix = folder_prefix
  buclet = client.get_bucket(bucket_name)
  blobs = bucket.list_blobs(prefix=folder_prefix)
  mp3_files = [blob.name for blob in blobs if blob.name.endswith(endswith)]
  mp3_uris = ["gs://" + str(bucket_name) + "/" + mp3 for mp3 in mp3_files [ 
  reutrn mp3_uris

def split_dual_channel(file_path:str):
  audio, sr = librosa.load(filepath, sr=None, mono=False)

  if audio.ndim == 2:
    left_channel = audio[0]
    right_channel = audio[1]

    sf.write("left_channel.wav", left_channel, sr)
    sf.write("right_channel.wav", right_channel, sr)
  else:
    print("Sin audio stereo")

def copy_blob(
      project_id:str, 
      bucket_name:str,
      blob_name:str,
      destination_bucket_name:str,
      destination_blob_name:str):
  storage_client = storage.Client(project=project_id)
  source_bucket = storage_client.bucket(bucket_name)
  source_blob = source_bucket.blob(blob_name)
  destination_bucket = storage_client.bucket(destination_bucket_name)
  destination_generation_match_precondition = 0

  blob_copy = source_bucket.copy_blob(
              source_blob, destination_bucket,destination_blob_name,
              if_generation_match=destination_generation_match_precondition
)

  print(
      "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
      source_blob.name,
      source_bucket.name,
      blob_copy.name,
      destination_bucket.name
  )
)
    
