import requests, json
import adal
import os
import argparse
import time

def get_aadtoken_spn(resource_app_id_url = "https://dev.azuresynapse.net/"):
    
    # Getting Authentication parameters from Environment variables
    tenant_id = os.environ['TENANT_ID']
    # Service Principal Client ID getting from Environment variables
    service_principal_id = os.environ['SPN_ID']

    # Service Principal Secret - Created in App Registrations from Azure Portal
    service_principal_secret = os.environ['SPN_SECRET']
    # Authority
    authority = f"https://login.windows.net/{tenant_id}"

    context = adal.AuthenticationContext(authority)
    token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)

    # Set Access Token from the toke Dict
    access_token = token["accessToken"]

    return access_token


def get_session_status(api_endpoint,sparkpool_name,session_id, api_version = "2022-02-22-preview"):
    aad_token = get_aadtoken_spn()
    try:
        response = requests.get(f"{api_endpoint}/livyApi/versions/{api_version}/sparkPools/{sparkpool_name}/sessions/{session_id}?detailed=True",
                                headers={'Authorization': f"Bearer {aad_token}","Content-Type": "application/json"})
    except requests.exceptions.HTTPError:
        raise Exception(response.text)
    session_status = response.json()
    return session_status

spark_session_resulttype = ["Cancelled","Failed","Succeeded","Uncertain"]
livy_states = ["busy","dead","error","idle","killed","not_started","recovering",
                "running","shutting_down","starting","success"]


def check_session_completion(api_endpoint,sparkpool_name,session_id):
    status = get_session_status(api_endpoint,sparkpool_name,session_id)
    if status["result"] == "Uncertain":
        return False
    else:
        return True

def wait_for_completion(api_endpoint,sparkpool_name,session_id, timewait = 30, time_out = 3600):
    timer = 0
    while not check_session_completion(api_endpoint,sparkpool_name,session_id):
        print(f"SessionId: {session_id} execution still in progress, sleeping")
        time.sleep(timewait)
        timer += timewait
        if timer > time_out:
            break

def execute_spark_jobdef(api_endpoint,spark_jobdef_name, run_sync = False, api_version = "2020-12-01"):
    aad_token = get_aadtoken_spn()

    try:
        response = requests.post(f"{api_endpoint}/sparkJobDefinitions/{spark_jobdef_name}/execute?api-version={api_version}",
                                 headers={'Authorization': f"Bearer {aad_token}","Content-Type": "application/json"})
    except requests.exceptions.HTTPError:
        raise Exception(response.text)

    job_exec_resp = response.json()
    job_id = job_exec_resp["id"]
    pool_name = job_exec_resp["sparkPoolName"]
    print(f"LivyJob with id {job_id} submitted on Spark Pool {pool_name}")
    if run_sync:
        wait_for_completion(api_endpoint,pool_name,job_id)        
    return job_id, pool_name

def check_env_vars():
    required_env_vars = ["TENANT_ID","SPN_ID","SPN_SECRET"]
    missing_env_var = [var for var in required_env_vars if not os.getenv(var)]
    missing_en_str = ", ".join(missing_env_var)
    if missing_env_var:
        raise Exception(f"Required environment variables: {missing_en_str} missing")
    

def parse_args(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--workspace_name', type=str, default='<your-workspace-name>')
    parser.add_argument('--operation', type=str, default='')
    parser.add_argument('--sparkjobdef', type=str, default='')
    parser.add_argument('--run_sync', type=bool, default=False)
    args_parsed = parser.parse_args(args_list)
    return args_parsed


if __name__ == "__main__":
    args = parse_args()
    workspace_name = args.workspace_name # "abguroo-synapse-ws" 
    operation = args.operation
    spark_jobdef_name =  args.sparkjobdef
    run_sync = args.run_sync
    api_endpoint = f"https://{workspace_name}.dev.azuresynapse.net"
    
    # Check if required Environment variables are present
    _ = check_env_vars()
    
    
    if operation == "execute_spark_job":
        #Execute a Spark Job definition        
        execute_spark_jobdef(api_endpoint,spark_jobdef_name, run_sync = run_sync, api_version = "2020-12-01")








