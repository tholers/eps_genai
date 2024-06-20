import azure.functions as func
import logging

#--new import
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

import os,io
import os.path, time
from io import BytesIO

import datetime as dt
import logging


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def get_list_subfolder(ctx,rootFolder):
    '''
    ctx: context
    '''
    libraryRoot = ctx.web.get_folder_by_server_relative_url(rootFolder)
    Subfolders = libraryRoot.folders
    ctx.load(Subfolders)
    ctx.execute_query()
    sub_folder = [i.properties['ServerRelativeUrl'] for i in Subfolders]

    return sub_folder


def get_list_file(ctx,rootFolder,last_extract_date,re_file=None):
    '''
    ctx: context
    last_extract_date: 
        -only get files from this date onwards (e.g. useful for incremental loading)
        -must be of the form '2024-04-23T17:00:00.000+00:00', so that we can .split('T')
    re_file: what is it? in file.properties['Name']
    '''
    libraryRoot = ctx.web.get_folder_by_server_relative_url(rootFolder)
    files = libraryRoot.files
    # files_modified = libraryRoot.time_last_modified
    ctx.load(files)
    ctx.execute_query()
    list_file = []
    for file in files:
        if re_file == None: #and file.properties['TimeLastModified'].split('T')[0]:# >= last_extract_date.split('T')[0]:
            list_file.append(file.properties['ServerRelativeUrl']) #get path of file is 'ServerRelativeUrl'        
            
            # it's a datetime.datetime object
            #print(file.properties['TimeLastModified'])
            # extract only date part
            # print(file.properties['TimeLastModified'].date())
            #convert date part to string
            print(dt.datetime.strftime(file.properties['TimeLastModified'].date(), "%Y-%m-%d"))
            
            # string with T, so that you can split('T')[0]
            #dt_str = dt.datetime.strftime(file.properties['TimeLastModified'], "%Y-%m-%dT%H:%M:%S")
            #print(d1)

            # last_extract_date already str
            # print(last_extract_date.split('T')[0])

        elif re_file.lower() in file.properties['Name'].lower() and file.properties['TimeLastModified'].split('T')[0] >= last_extract_date.split('T')[0]:#get data be modified from now date - 1
            list_file.append(file.properties['ServerRelativeUrl']) #get path of file is 'ServerRelativeUrl'
           
    return list_file




@app.route(route="HttpExample")
def HttpExample(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    #name = req.params.get('name')
    a = req.params.get('a')
    b = req.params.get('b')

    # Application ID and secret
    client_id       = os.environ["client_id"]
    client_secret   = os.environ["secret_value"]

    # Configuration to connect blob storage
    # req_body = req.get_json() # need to send a json, o/w error

    # connection_string = os.environ["storage_account"]
    # container_name = os.environ["blob_container"]              
    # service = BlobServiceClient.from_connection_string(conn_str=connection_string)

    site_url = os.environ["sharepoint_site_url"]

    # Get access_token using app principal
    app_principal = {
        "client_id": client_id,
        "client_secret": client_secret,
    }

    ctx_auth = AuthenticationContext(site_url)
    ctx_auth.acquire_token_for_app(
        client_id=app_principal["client_id"], client_secret=app_principal["client_secret"]
    )

    #API get objects in Sharepoint
    ctx = ClientContext(site_url, ctx_auth)

    # -------Testing with sub folders 
    list_folder = get_list_subfolder(ctx, rootFolder="EPS Filing System")
    list_folder.sort()
    # print(sub_folder)

    # return func.HttpResponse(f"Testing.", status_code=200)

    # return func.HttpResponse(
    #     f"This HTTP triggered function executed successfully.\n {list_folder}",
    #     status_code=200
    # )

    # -------Testing with files
    sharepoint_folder = "EPS Filing System/VOYAGE FILE/EPS23652/BBB/9 - Miscellaneous Communication"
    last_extract_date = '2020-01-01T00:00:00'
    re_file = None
    list_file = get_list_file(ctx,
                              rootFolder = sharepoint_folder,
                              last_extract_date = last_extract_date,
                              re_file = re_file)
    
    # list_file = ['123']
    return func.HttpResponse(
        f"This HTTP triggered function executed successfully.\n {list_file[0]}",
        status_code=200
    )    

