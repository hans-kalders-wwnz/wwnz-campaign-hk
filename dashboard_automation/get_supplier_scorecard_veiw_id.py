import tableauserverclient as TSC
import json
from google.cloud import secretmanager

# Set up client to access Tableau Credentials
client = secretmanager.SecretManagerServiceClient()
project_id = "gcp-wow-pel-de-lab-dev"
secret_name = "tableau-credentials"
version_id = 4

# Access the secret and get the username and password values
secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/{version_id}"
secrect_response = client.access_secret_version(name = secret_path)
secret = json.loads(secrect_response.payload.data.decode("UTF-8"))
# username = secret["username"]
# password = secret["password"]
token_name = secret["token_name"]
personal_access_token = secret["personal_access_token"] 


site_id = "woolworthsnewzealand"

# Set up Tableau Server connection
# tableau_auth = TSC.TableauAuth(username, password, site_id)
tableau_auth = TSC.PersonalAccessTokenAuth(token_name, personal_access_token, site_id=site_id)
server = TSC.Server('https://us-west-2b.online.tableau.com')
server.auth.sign_in(tableau_auth)

# Get the workbook and view
workbook_name = "Supplier Scorecard"
view_name = "Supplier Scorecard"
workbook = None
view = None
wbs = TSC.Pager(server.workbooks)
for wb in wbs:
    if wb.name == workbook_name:
        workbook = wb
        break
if workbook is not None:
    for v in TSC.Pager(server.views):
        if v.name == view_name and v.workbook_id == workbook.id:
            view = v
            break

# Print the view ID
if view is not None:
    print(f"The view ID of {view_name} in workbook {workbook_name} is {view.id}. \n{view_name} URL is {view.content_url}")
else:
    print(f"Could not find view {view_name} in workbook {workbook_name}")
