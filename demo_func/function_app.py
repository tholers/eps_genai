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


def get_list_subfolder_v2(ctx,rootFolder, recursive=False):
    '''
    ctx: context
    '''
    final_subfolder = [] # store all subfolders recursively

    # get the subfolders of rootFolder only
    subfolder = ctx.web.get_folder_by_server_relative_url(rootFolder).folders
    ctx.load(subfolder)
    ctx.execute_query()
    
    # for each subfolder above, recursively add all its subfolders
    if recursive == True:
        for folder in subfolder:
            final_subfolder.extend(
                get_list_subfolder_v2(
                    ctx = ctx,
                    rootFolder = folder.properties['ServerRelativeUrl'],
                    recursive = recursive
                )
            )

    final_subfolder.extend([i.properties['ServerRelativeUrl'] for i in subfolder])

    return final_subfolder


def get_list_file_v2(ctx,rootFolder,last_extract_date,re_file=None, recursive=False):
    '''
    ctx: context
    last_extract_date: 
        -only get files from this date onwards (e.g. useful for incremental loading)
        -must be of the form '2024-04-23T17:00:00.000+00:00', so that we can .split('T')
    re_file: what is it? in file.properties['Name']
    '''
    final_file = []# store all files recursively
    files = ctx.web.get_folder_by_server_relative_url(rootFolder).files
    ctx.load(files)
    ctx.execute_query()

    final_file.extend([file.properties['ServerRelativeUrl'] for file in files if
                       dt.datetime.strftime(file.properties['TimeLastModified'].date(), "%Y-%m-%d") 
                       >= last_extract_date.split('T')[0] ])
    
    
    final_subfolder = []

    subfolder = ctx.web.get_folder_by_server_relative_url(rootFolder).folders
    ctx.load(subfolder)
    ctx.execute_query()
    
    if recursive == True:
        final_subfolder = get_list_subfolder_v2(
                    ctx = ctx,
                    rootFolder = rootFolder,
                    recursive = False
        ) # recusive should be false here, since the recusive property is already
        # taken care by the following get list file logic already.
        # if we use recursive=True here, it will keep repeating subfolders

        for folder in final_subfolder:
            final_file.extend(
                get_list_file_v2(
                    ctx = ctx,
                    rootFolder = folder,
                    last_extract_date = last_extract_date,
                    re_file=None,
                    recursive = recursive
                )
            )

    #final_subfolder.extend([i.properties['ServerRelativeUrl'] for i in subfolder])
    #ctx.load()
    #final_file.extend([i.properties['ServerRelativeUrl'] for i in subfolder])
    #final_subfolder = [i.properties['ServerRelativeUrl'] for i in subfolder]

    return final_file


def download_file(ctx,rootFolder,connection_string,container_name,service, recursive=False):
    '''
    '''
    final_file = []# store all files recursively
    files = ctx.web.get_folder_by_server_relative_url(rootFolder).files
    ctx.load(files)
    ctx.execute_query()
    final_file.extend([file.properties['ServerRelativeUrl'] for file in files])
    
    blob_folder = "test2"
    for file_url in final_file:
        bytes_file_obj = io.BytesIO()
        ctx.web.get_file_by_server_relative_path(file_url).download(bytes_file_obj).execute_query()
        file_name = file_url.split('/')[-1]
        # blob_folder_batch = file_url.partition("VOYAGE FILE")[-1]
        # first, get rid of everything before and including "VOYAGE FILE"
        # then extract only the dir of the remaining (i.e. get rid of file part)
        blob_folder_batch = os.path.dirname(file_url.partition("VOYAGE FILE")[-1])

        # where to put the file in blob folder
        # path_new_file = f'{blob_folder}/{file_name}'   
        path_new_file = f'{blob_folder}/{blob_folder_batch}/{file_name}' 
        blob_client = service.get_blob_client(container=container_name,blob = path_new_file)
        blob_client.upload_blob(bytes_file_obj.getvalue(),overwrite=True)

    return final_file


@app.route(route="HttpExample")
def HttpExample(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Application ID and secret
    client_id       = os.environ["client_id"]
    client_secret   = os.environ["secret_value"]

    # Configuration to connect blob storage
    # req_body = req.get_json() # need to send a json, o/w error
    # print(req_body.get("a"))
    # print(req_body.get("b"))

    connection_string = os.environ["storage_account"]
    container_name = os.environ["blob_container"]              
    service = BlobServiceClient.from_connection_string(conn_str=connection_string)

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
    # # list_folder = get_list_subfolder(ctx, rootFolder="EPS Filing System/VOYAGE FILE/ADRIATIC SEA/1004/20")
    # # list_folder = get_list_subfolder(ctx, rootFolder="EPS Filing System/VOYAGE FILE/ADRIATIC SEA/1004/20/9 - Miscellaneous Communication")
    # list_folder = get_list_subfolder_v2(ctx, 
    #                                     rootFolder="EPS Filing System/VOYAGE FILE/ADRIATIC SEA/",
    #                                     recursive=True)
    # list_folder.sort() # for about 20 subfolders, total time is 11s, where the sort() not take much
    # # print(sub_folder)

    # return func.HttpResponse(
    #     f"This HTTP triggered function executed successfully.\n {list_folder}",
    #     status_code=200
    # )

    # -------Testing with files
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/EPS23652/BBB/9 - Miscellaneous Communication"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/ADRIATIC SEA/1004/20/1 - Voyage Fixture"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/ADRIATIC SEA/1004/20/"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/ADRIATIC SEA/1006"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/ADRIATIC SEA/1004/"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/ADRIATIC SEA/"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/ATLANTIC EMERALD"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/"
    
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/AMALFI BAY"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/BRIGHTWAY"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/CARTIER"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/CMA CGM ARCTIC"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/MOUNT GAEA"
    # sharepoint_folder = "EPS Filing System/VOYAGE FILE/SERENE SEA"
    sharepoint_folder = "EPS Filing System/VOYAGE FILE/ZIM ARIES"
    
    last_extract_date = '2020-01-01T00:00:00'
    # last_extract_date = '2024-06-21T00:00:00'
    # last_extract_date = '2024-01-01T00:00:00'
    # last_extract_date = '2024-07-01T00:00:00'
    re_file = None
    
    list_file = get_list_file_v2(ctx,
                              rootFolder = sharepoint_folder,
                              last_extract_date = last_extract_date,
                              re_file = re_file,
                              recursive=True)
    
    print('done getting list file, now working on downloading file')
    blob_folder = "test2"
    count_files = 0
    for file_url in list_file:
        # print(file_url)
        # print(1)
        bytes_file_obj = io.BytesIO()

        ctx.web.get_file_by_server_relative_path(file_url).download(bytes_file_obj).execute_query()
        file_name = file_url.split('/')[-1]
        blob_folder_batch = os.path.dirname(file_url.partition("VOYAGE FILE")[-1])[1:]

        # where to put the file in blob folder
        # path_new_file = f'{blob_folder}/{file_name}' 
        path_new_file = f'{blob_folder}/{blob_folder_batch}/{file_name}'  
        # print(path_new_file)
        blob_client = service.get_blob_client(container=container_name,blob = path_new_file)

        blob_client.upload_blob(bytes_file_obj.getvalue(),overwrite=True)
        

        count_files += 1
        print(f'done uploading {count_files} files')

    print('done uploading all files')

    # list_file = download_file(ctx,
    #                         rootFolder = sharepoint_folder,
    #                         connection_string=connection_string,
    #                         container_name=container_name,
    #                         service=service,
    #                         recursive=False)

    return func.HttpResponse(
        f"This HTTP triggered function executed successfully.\n {len(list_file), list_file}",
        status_code=200
    )    

