from flask import Flask, request, jsonify
from google.cloud import storage
import os
import json
from google.cloud.bigquery import LoadJobConfig, SchemaField
from google.cloud import bigquery 

"""
param to use api
file_csv_load : ['departments.csv','jobs.csv','hired_employees.csv'] to user a repository
file_path   : files_csv
file_bucket : "globant-excersices-demo"
file_proyect : local-snow-414715
file_dataset_bq : globant_raw_tables
"""
 

def load_tables_bq(proj_id,dataset,table_name,bucket):

    schema_files = 'schema_tables'
    bq_client = bigquery.Client(project=proj_id)
    client = bq_client
    t_name = str(table_name).lower().replace(".csv","").replace("./","").replace(".txt","").replace("./","")
    path_file = os.path.join(schema_files, t_name)
    
    table_id = proj_id+"."+dataset+"."+t_name
    uri = "gs://"+bucket+"/"+table_name
    path_file = os.path.join(schema_files, t_name+".json")
    print(path_file)
    with open(path_file) as f:
        file_schema_bq = json.load(f)
    
    table_schema = [ SchemaField(field["name"], field["type"], description=field.get("description", ""))
    for field in file_schema_bq
    ]
    
    job_config = bigquery.LoadJobConfig(
             skip_leading_rows=1,
             write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
             source_format=bigquery.SourceFormat.CSV,
             autodetect=True,
             schema=table_schema,
             field_delimiter=',',
             max_bad_records=100000000,
             ignore_unknown_values=True,
             preserve_ascii_control_characters=True,
             null_marker='\u0000',
             quote_character='"'
    )
    print("##############################")
    print(table_id)
    load_job = client.load_table_from_uri( uri, 
                                           table_id, 
                                           job_config=job_config
                )   
    load_job.result()
    return 0   
#----------------------------------------------------------------------------------------------------

app = Flask(__name__)

# Ruta para subir un archivo CSV al bucket de GCS
@app.route("/upload", methods=["POST"])
def upload_file_to_gcs():
    req = request.json
    file_csv_load = req['file_csv_load']
    file_path = req['file_path']
    file_bucket = req['file_bucket']
    file_proyect_id = req['file_proyect_id']
    file_bq_dataset = req['file_bq_dataset']  
     
      
    storage_client = storage.Client(project=file_proyect_id)
    # Proyect ID client
    cliente_gcs = storage_client
    # bucket
    bucket = cliente_gcs.bucket(file_bucket)
    
# archivos CSV en el directorio
    for nombre_archivo in os.listdir(file_path):
        if nombre_archivo.endswith(".csv") or nombre_archivo.endswith(".CSV"):
            # Ruta completa del archivo CSV
            ruta_archivo_csv = os.path.join(file_path, nombre_archivo)                         
            # Subir el archivo CSV al bucket
            blob = bucket.blob(nombre_archivo)
            blob.upload_from_filename(ruta_archivo_csv) 
            load_tables_bq(file_proyect_id,file_bq_dataset,nombre_archivo,file_bucket)  
                      
    
    return jsonify({"mensaje": "Archivo cargado"})

if __name__ == "__main__":
    app.run(debug=True)
