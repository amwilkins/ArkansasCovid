import pickle
from googleapiclient.discovery import build

SPREADSHEET_ID = '16mdoWm0k3JUllm3InhoD_Jiq0lbArO_UGum8NMp9USU' # Get this one from the link in browser
worksheet_name = 'Date Master API testing'
path_to_csv = 'arc_master.csv'
path_to_credentials = "token.pickle"

print("Starting upload to Google Sheets")
# convenience routines
def find_sheet_id_by_name(sheet_name):
    # ugly, but works
    sheets_with_properties = API \
        .spreadsheets() \
        .get(spreadsheetId=SPREADSHEET_ID, fields='sheets.properties') \
        .execute() \
        .get('sheets')

    for sheet in sheets_with_properties:
        if 'title' in sheet['properties'].keys():
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']


def push_csv_to_gsheet(csv_path, sheet_id):
    with open(csv_path, 'r') as csv_file:
        csvContents = csv_file.read()
    body = {
        'requests': [{
            'pasteData': {
                "coordinate": {
                    "sheetId": sheet_id,
                    "rowIndex": "0",  # adapt this if you need different positioning
                    "columnIndex": "0", # adapt this if you need different positioning
                },
                "data": csvContents,
                "type": 'PASTE_NORMAL',
                "delimiter": ',',
            }
        }]
    }
    request = API.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body)
    response = request.execute()
    return response


# upload
with open(path_to_credentials, 'rb') as token:
    credentials = pickle.load(token)

API = build('sheets', 'v4', credentials=credentials)

push_csv_to_gsheet(
    csv_path=path_to_csv,
    sheet_id=find_sheet_id_by_name(worksheet_name)
)
print("Upload complete")
