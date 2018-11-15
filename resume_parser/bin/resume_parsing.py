from __future__ import print_function
import logging
import os, io
import pandas
import textract


import lib
import field_extraction
import spacy


from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import auth
import file_io
import sys
sys.path.append('../')
#from dao.db_connection import db_connection
from dao.batch import batch
#from utils.common_utils import common_utils
from dao.files_processed import files_processed
from dao.users import users
from dao.skills import skills


def processResumes(connection):
    """
    Main function documentation template
    :return: None
    :rtype: None
    """
    SCOPES = 'https://www.googleapis.com/auth/drive.readonly'
    CREDENTIALS_FILE = 'credentials.json'
    authInst = auth.auth(SCOPES, CREDENTIALS_FILE)
    creds = authInst.getCredentials()
    drive_service = build('drive', 'v3', http=creds.authorize(Http()))
    resumes_destination_path = "../data/input/example_resumes/"
    DRIVE_FOLDER_NAME = 'resumes'
    
    logging.getLogger().setLevel(logging.INFO)
    fileIOInst = file_io.file_io(drive_service)
   
    #Search folder with name
    folderId = fileIOInst.getFolderId(DRIVE_FOLDER_NAME)    
    
    #Get all files in folder
    files = fileIOInst.getAllFiles(folderId)
    result = {}
    result['data'] = {}
    if len(files) > 0 :
        fileProcessedObj = files_processed(connection)
        alreadyProcessedFiles = 0
        filesToProcessArr = {}
        for  remoteId in files:
            cursor = fileProcessedObj.find(remoteId)    
            if (cursor != False and cursor.rowcount > 0) :
                logging.info('File already processed:'+files[remoteId])
                alreadyProcessedFiles = alreadyProcessedFiles+1
            else :
                filesToProcessArr[remoteId] = files[remoteId]
        
        if (len(filesToProcessArr) > 0) :
            # Spacy: Spacy NLP
            nlp = spacy.load('en')
            
            #Create batch 
            batchObj = batch(connection)
            batchId = batchObj.create(len(filesToProcessArr), alreadyProcessedFiles)
            if (batchId > 0) :
                result['data']['batch_id'] = batchId
                #Download single file and process
                batchObj.update(batchId, 'In-process')
                for remoteId in filesToProcessArr:
                    fileId = fileProcessedObj.create(remoteId, batchId)  
                    fileIOInst.downloadSingleFile(remoteId, resumes_destination_path+filesToProcessArr[remoteId])
                    
                    # Extract data from upstream.
                    observations = extract()
                    
                    # Transform data to have appropriate fields
                    observations, nlp = transform(observations, nlp)
                    
                    # Load data for downstream consumption
                    load(connection, observations, nlp, fileId)
                            
                    fileIOInst.cleanInputDir(resumes_destination_path) 
                
                batchObj.update(batchId, 'Finished')
                
            else:
                logging.error("Unable to create batch")
                
            
        else :
            result['status'] = 'failure'
            result['data']['msg'] = 'No files to process'
            logging.info("All files are already processed")  
                    
    return result


def text_extract_utf8(f):
    try:
        return str(textract.process(f), "utf-8")
    except UnicodeDecodeError:
        return ''

def extract():
    logging.info('Begin extract')

    # Reference variables
    candidate_file_agg = list()

    # Create list of candidate files
    for root, subdirs, files in os.walk(lib.get_conf('resume_directory')):
        folder_files = map(lambda x: os.path.join(root, x), files)
        candidate_file_agg.extend(folder_files)

    # Convert list to a pandas DataFrame
    observations = pandas.DataFrame(data=candidate_file_agg, columns=['file_path'])
    logging.info('Found {} candidate files'.format(len(observations.index)))

    # Subset candidate files to supported extensions
    observations['extension'] = observations['file_path'].apply(lambda x: os.path.splitext(x)[1])
    observations = observations[observations['extension'].isin(lib.AVAILABLE_EXTENSIONS)]
    logging.info('Subset candidate files to extensions w/ available parsers. {} files remain'.
                 format(len(observations.index)))

    # Attempt to extract text from files
    observations['text'] = observations['file_path'].apply(text_extract_utf8)
    #observations['text'] = ''
    # Archive schema and return
    lib.archive_dataset_schemas('extract', locals(), globals())
    logging.info('End extract')
    return observations


def transform(observations, nlp):
    # TODO Docstring
    logging.info('Begin transform')

    # Extract contact fields
    observations['email'] = observations['text'].apply(lambda x: lib.term_match(x, field_extraction.EMAIL_REGEX))
    observations['phone'] = observations['text'].apply(lambda x: lib.term_match(x, field_extraction.PHONE_REGEX))
    
    # Extract candidate name
    observations['candidate_name'] = observations['text'].apply(lambda x:
                                                                field_extraction.candidate_name_extractor(x, nlp, observations['email']))
    # Extract skills
    observations = field_extraction.extract_fields(observations)
    # Archive schema and return
    lib.archive_dataset_schemas('transform', locals(), globals())
    logging.info('End transform')
    return observations, nlp


def load(connection, observations, nlp, fileId):
    logging.info('Begin load')
    output_path = os.path.join(lib.get_conf('summary_output_directory'), 'resume_summary.csv')

    logging.info('Results being output to {}'.format(output_path))
    del(observations['text'])
    observations.to_csv(path_or_buf=output_path, index_label='index', encoding='utf-8', sep=",", quotechar='"')
    userObj = users(connection)
    skillsObj = skills(connection)
    for index, row in observations.iterrows():
        sk = {}
        sk["platforms"] = row['platforms']
        sk["database"] = row['database']
        sk["programming"] = row['programming']
        sk["machinelearning"] = row['machinelearning']
        userSkills = {}
        userSkills['experience'] = row['experience']
        userSkills['skills'] = sk
        userCursor = userObj.find({'email': row['email']})
        if userCursor.rowcount > 0 :
            user = userCursor.fetchone() 
            userId = user['id']
            logging.info("Users with email:"+ row['email']+ " already exist - updating user:"+ str(userId))
            userObj.update(row, fileId, user['id'])                
        else :    
            userId = userObj.create(row, fileId)
            
        skillsObj.create(userSkills, userId)
        
    logging.info('End transform')
    pass

# Main section
#if __name__ == '__main__':
#    main()
