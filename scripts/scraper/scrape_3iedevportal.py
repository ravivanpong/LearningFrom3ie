## LICENSE  :   MIT License
##
## PURPOSE  :   Python script to scrape 3ie development portal for impact evaluation reports and
##              systematic reviews. The scraped data are saved into MongoDB. 
##
## AUTHOR   :   Ployplearn Ravivanpong (p.ravivanpong@e.mail.de)
## 
## NOTE     :   The script synchronously downloads the metadata (overview data, containing ID, title,
##              synopsis, year of publication). This could have been done asynchronously. As a beginner in
##              web-scraping in Python, I used the package "requests" first, due to its ease-of-use and found out later
##              that it does not support asynchronous requests. Since ca. 100 requests to be made in the first
##              scraping and I expect less than 10 requests for subsequent updates, I see no compelling reason
##              to rewrite the code for asynchronous scraping.
##
## PROBLEM   :  - There is a problem with droping duplicates based on "id". By doing so, we missed 
##                8 impact evaluations 
##              - One of the impact evaluation contains empty record details.    

import requests
import time
from datetime import datetime
from pymongo import MongoClient
import asyncio
import aiohttp
import logging
import os


## Global variables and settings
##====================================================================================
URL_GRAHPQL = 'https://api.developmentevidence.3ieimpact.org//graphql'

## It is advised that the json_data structure is exactly the same as from the website to avoid encountering
## prohibited queries. We can get the structure by examining the Network of the website.
## See https://scrapfly.io/blog/web-scraping-graphql-with-python/ and in the documentation on how to find this.

## Query for all sysmetic reviews and impact evaluation reports. We will use the 'id' to 
## query each record detail later.
QUERY_TEMPLATE_KEYWORDSEARCH = {
    "query": """query KEYWORD_SEARCH($data: KeywordSearchInput!) {
        keywordSearch(data: $data) {
            total_count
            search_result {
                id
                product_type
                title
                short_title
                synopsis
                sector_name
                status
                open_access
                keywords
                year_of_publication
                impact_evaluations
                systematic_reviews
                dataset_url
                }
            }
        }
        """
        , "variables":{
            "data": {
                "from": 0,
                "keyword": "*",
                "size": "150",
                "sort_by": "relevance",
                "filters": {
                "product_type": [
                    "srr",
                    "ier"
                ],
                "sector_name": [],
                "continents": [],
                "threeie_funded": [],
                "fcv_status": [],
                "countries": [],
                "equity_dimension": [],
                "primary_theme": [],
                "equity_focus": [],
                "year_of_publication": [],
                "dataset_available": [],
                "primary_dac_codes": [],
                "un_sdg": [],
                "primary_dataset_availability": [],
                "pre_registration": [],
                "interventions": [],
                "outcome": [],
                "evaluation_method": [],
                "confidence_level": [
                    "Medium",
                    "High",
                    "Low"
                ]}
            }
        }
}

## Query for details of each record
QUERY_TEMPLATE_RECORDDETAILS = {
    "query":"""query recordDetail($id: Int!) {
        recordDetail(id: $id) {
            product_type
            title
            synopsis
            id
            short_title
            language
            sector_name
            sub_sector
            journal
            journal_volume
            journal_issue
            year_of_publication
            publication_type
            publication_url
            egm_url
            report_url
            grantholding_institution
            evidence_programme
            context
            research_questions
            main_finding
            review_type
            quantitative_method
            qualitative_method
            overall_of_studies
            overall_of_high_quality_studies
            overall_of_medium_quality_studies
            headline_findings
            additional_method
            additional_method_2
            pages
            evaluation_design
            impact_evaluations
            systematic_reviews
            authors {
                author
                institutions {
                author_affiliation
                department
                author_country
                }
            }
            continent {
                continent
                countries {
                country
                income_level
                fcv_status
                }
            }
            project_name {
                project_name
                implementation_agencies {
                implementation_agency
                implement_agency
                }
                funding_agencies {
                program_funding_agency
                agency_name
                }
                research_funding_agencies {
                research_funding_agency
                agency_name
                }
            }
            research_funding_agency {
                research_funding_agency
                agency_name
            }
            publisher_location
            status
            threeie_funded
            is_bookmark
            based_on_the_above_assessments_of_the_methods_how_would_you_rate_the_reliability_of_the_review
            provide_an_overall_of_the_assessment_use_consistent_style_and_wording
            abstract
            open_access
            doi
            equity_focus
            equity_dimension
            equity_description
            keywords
            evaluation_method
            mixed_methods
            unit_of_observation
            methodology
            methodology_summary
            main_findings
            evidence_findings
            policy_findings
            research_findings
            background
            objectives
            region
            stateprovince_name
            district_name
            citytown_name
            location_name
            other_resources
            additional_url {
                additional_url_address
                additional_url
            }
            relatedArticles {
                product_id
                title
                product_type
            }
            dataset_url
            dataset_available
            instances_of_evidence_use
            study_status
            primary_dac_code
            secondary_dac_code
            crs_voluntary_dac_code
            un_sustainable_development_goal
            primary_dataset_url
            pre_registration_url
            primary_dataset_availability
            primary_dataset_format
            secondary_dataset_name
            secondary_dataset_disclosure
            additional_dataset_info
            analysis_code_availability
            analysis_code_format
            study_materials_availability
            study_materials_list
            pre_registration
            protocol_pre_analysis_plan
            ethics_approval
            interventions
            outcome
            }
        }
    """
    , "variables": {
        "id": "21693"
        }
}
##----------------------------------------------------------------------------------------------

## Helper Functions
##==============================================================================================
def request_graphql(url: str, json_query: dict) -> dict:
    """
    Send HTTP POST requests to retrieve data using Graphql query. Return the response as a a dictionary
    to be save in MongoDB.
    
    Parameters
    -----------
    url :  str
        The URL to the Graphql, e.g. 'https://api.developmentevidence.3ieimpact.org//graphql'
    
    json_query : dict
        A dictionary containing Graphql query format. It is advised to use the same query format as the target
        website to avoid errors.

    Returns
    --------
    dict :  The response of the HTTP POST request. The dictionary is suitable for saving directly into MongoDB.
    """
    attempt = 0
    while attempt < 3:
        response = requests.post(url, json=json_query)
        if response.status_code == 200:
            return response.json()
        else: 
            print("Request returns error. Try again.")
            time.sleep(3)
            attempt += 1


def save_to_mongodb(collection: object, data) -> None:
    """
    Save the datapoint(s) into MongoDB collection

    Parameters
    ----------
    collection : PyMongo collection object

    data : dict, list
        A document or list of documents to add into the collection

    Returns
    -------
    None
    """
    if isinstance(data, dict):
        collection.insert_one(data)
    else:
        collection.insert_many(data)
    print("Data saved to MongoDB")


def drop_duplicates(collection, by) -> None:
    """
    Drop duplicates in the collection based on a field in the MongoDB collection, keeping only the first entry.

    Parameters
    ----------
    collection : PyMongo collection object

    by : str
        Name of the field in MongoDB collection without "$" prefix.
    
    Returns
    -------
    None : The function modifies the MongoDB collection.
    """
    orig_num_docs = collection.count_documents({})
    id_field = '$' + by
    pipeline = [
        {## 1. Group documents by 'id', save the '_id' (unique, created by MongoDB when the documents are inserted)
        ## then count the number of documents with the same 'id'    
            '$group': {
                '_id': id_field,
                'duplicates': {'$push': '$_id'},
                'count': {'$sum': 1}
            }
        },
        {## 2. Find groups that have more than 1 documents (duplicates)
            '$match': {
                'count': {'$gt': 1}
            }
        }
    ]

    cursor = collection.aggregate(pipeline)
    docs = list(cursor)
    if len(docs) > 0:
        print("Duplicates found. Remove duplicates....")
        ids_to_remove = []
        for doc in docs:
            ids_to_remove.extend(doc['duplicates'][1:])

        result = collection.delete_many({'_id':{'$in':ids_to_remove}})
        num_after_drop_duplicates = collection.count_documents({})
        print(f"""
        Before removing duplicates: {orig_num_docs} documents
        Number of duplicates deleted: {result.deleted_count} documents
        After removing duplicates: {num_after_drop_duplicates} documents
        """)
    else:
        print("No duplicates found.")

def scrape_meta_data(collection: object, url: str, json_query: dict = None, update: bool = True) -> None:
    """
    Scrape the meta data of the evidences. If update == True, then scrape only the reports published from the
    most recent year-of-publication in the specified collection.

    Parameters
    ----------
    collection : PyMongo collection object
    url : str
        The URL to Graphql

    json_query : dict
        Graphql query

    update : bool
        If True, then scrape only the reports published from the most recent year-of-publication in the collection.

    Returns
    --------
    None : The function modifies the collection
    """
    ## Steps:
    ## 1) Send HTTP POST request with the json_data
    ## 2) Get the response of the first 150 entries (highest limits given by the website information)
    ## 3) Travel the nested dict to find 'total_count'.
    ## 4) Paginate the search result by moidfying the 'from' in json_data until we reach the 'total_count'

    ## If a query is not given, use the default global query for meta data 'QUERY_TEMPLATE_KEYWORDSEARCH'. Work on copy.
    if json_query is not None:
        json_data = json_query.copy()
    else: 
        json_data = QUERY_TEMPLATE_KEYWORDSEARCH.copy()

    ## Initial request to test the POST and get total_count
    dict_response = request_graphql(url=url, json_query=json_data)

    ## Check if requests return error.
    ## If the query fails, the response will contain key "errors." The response.status_code will still return 200 ("OK")
    if dict_response.keys() == 'errors':
        print(dict_response['errors'])
        raise ValueError("Bad queries. See the print of response_json['errors']")

    total_count = dict_response['data']['keywordSearch']['total_count']

    def get_all_entries(dict_response, max_size = 150):
            ## Save initial reponse
            search_result = dict_response['data']['keywordSearch']['search_result']
            save_to_mongodb(collection=collection, data=search_result)

            ## Get the next max_size entries until all entries are downloaded.
            if max_size <= total_count:
                for entry in range(len(search_result)+1, total_count, max_size):
                    json_data['variables']['data']['from'] = entry  ## Modify the query to get the next 150 entries
                    entry_end = min(entry + max_size, total_count)
                    print(f"Requesting entry {entry} to {entry_end}...")
                    dict_response = request_graphql(url=url, json_query=json_data)
                    search_result = dict_response['data']['keywordSearch']['search_result']
                    save_to_mongodb(collection=collection, data=search_result)

    ## If update is False, then scrape everything again.
    if update != True:
        print(f"Scrape everything ({total_count} documents)).....")             
        get_all_entries(dict_response, max_size = 150)

    else:  ## Scrape only documents that are published from the most recent year-of-publication in our local
           ## database.
        print("Update collection.....")
        collection_total_count = collection.count_documents(filter={})
        print(f"""
        Number of documents in the local collection = {collection_total_count}
        Number of documents (impact eval + systematic review) in 3ie development = {total_count}
        """)

        if total_count == collection_total_count:
            ## Do nothing
            print("Nothing to update.")
        
        else:
            ## As of 06.2023, The 3ie development portal does not provide filter variables to select documents from a specified
            ## created_date. A work-around is to retrieve only reports published from the latest_year_of_publication
            ## in the local database. This will create duplicates that is dropped in the post-processing.
            latest_year_of_publication = int(collection.find({'year_of_publication':{'$ne':"999"}}
                                                             , sort=[('year_of_publication',-1)]
                                                             , limit=1)[0]['year_of_publication'])
            
            ## Select more recent year of publication and including unknown years (just to be safe side)
            select_years = list(range(latest_year_of_publication, datetime.now().year+1, 1))
            json_data['variables']['data']['filters']['year_of_publication'] = [*select_years, 999]

            ## Send a modified "initial" request to get only documents from the selected years.
            dict_response = request_graphql(url=url, json_query=json_data)
            total_count = dict_response['data']['keywordSearch']['total_count']
            
            ## Loop to get all entries, if the result contain more than 150 entries.
            get_all_entries(dict_response, max_size = 150)

    print("---- Finish scraping ----")    
            
    ## Drop duplicates based on 'id', just in case the collection already exists.
    drop_duplicates(collection, 'id')
    


def get_report_ids(collection: object, ascending: bool =True) -> list:
    """
    Get the list of report IDs.

    Parameters
    ----------
    collection : PyMongo collection object

    ascending : bool
        If True, sort IDs by ascending order

    Returns
    -------
    list[str] : List of report IDs. The IDs are of type str.
    """
    ## Sort by ascending IDs
    if ascending:
        sort_direction = 1
    else:
        sort_direction = -1

    list_dict_ids = list(collection.find(filter={}, projection={'id':True, '_id':False}, sort=[('id',sort_direction)]))
    list_ids = []
    if list_dict_ids != False:  ## If report IDs exist in the collection. Return empty list otherwise.
        for item_dict in list_dict_ids:
            list_ids.append(item_dict['id'])
    return list_ids


def compare_records(collection_a: object, collection_b: object) -> list:
    """
    Find report IDs that only exist in collection_a. This assume that IDs in collection_b is a subset collection_a.
    
    Parameters
    ----------
    collection_a : PyMongo collection object
        Metadata collection that is used to get an overview of all the reports in the portal. 

    collection_b : PyMongo collection object
        The collection contains the record details from the reports that have been scraped so far.

    Returns
    -------
    list(str) : List of IDs that exist only in collection_a
    """
    id_metadata = get_report_ids(collection=collection_a, ascending=True)
    id_records = get_report_ids(collection=collection_b, ascending=True)
    return list(set(id_metadata)-set(id_records))


async def async_save_to_mongodb(collection, data):
    if isinstance(data, dict):
        collection.insert_one(data)
    else:
        collection.insert_many(data)

async def async_post_and_save(collection, url, json_query):
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=json_query) as response:
            response_json = await response.json()
            data = response_json['data']['recordDetail']
    await async_save_to_mongodb(collection, data)

async def async_scrape_records(collection, record_ids=None, request_limit=10
                               , url=None, json_query=None):
    
    ## Do nothing if no record IDs are given.
    if record_ids is None:
        print("No record IDs given. Scrape not executed.")
        return None
    
    ## Default URL
    if url is None:
        url = 'https://api.developmentevidence.3ieimpact.org//graphql'

    if json_query is None:
        json_query = QUERY_TEMPLATE_RECORDDETAILS


    json_data = json_query.copy()
    semaphore = asyncio.Semaphore(request_limit)  ## Set number of request limit
    
    async def get_record(id):
        json_data['variables']['id'] = id
        await async_post_and_save(collection, url, json_query=json_data)

    ## Create tasks for asynchronous requests with request limit.
    async with semaphore:
        tasks = [asyncio.create_task(get_record(id)) for id in record_ids]
        
        ## Report status
        while len(asyncio.all_tasks()) > 1:  ## The script stops when number of task = 1
            print(f"Current tasks pool {len(asyncio.all_tasks())}")
            await asyncio.sleep(5)
        
        ## Execute tasks
        await asyncio.wait(tasks)
        print(f"Done: Current tasks pool {len(asyncio.all_tasks())}")


## Main script ------------------------------------------------------------
def main(update=True):
    client_name = 'mongodb://localhost:27017/'
    database_name = '3ieData'
    collection_id_name = 'evidence_id'
    collection_records_name = 'evidence_records'

    ## Connect to the database --------------------------
    ## Note: MongoDB will create a database and a collection if they do not exist when a data point
    ##       is inserted to it. It will not warn users about this.
    client = MongoClient(client_name)
    database = client[database_name]
    collection_id = database[collection_id_name]
    collection_records = database[collection_records_name]    

    scrape_meta_data(collection=collection_id
                    , url=URL_GRAHPQL, json_query=QUERY_TEMPLATE_KEYWORDSEARCH, update=update)

    ## Scrape only the records that do not exist in the metadata collection
    records_to_scrape = compare_records(collection_id, collection_records)
    current_counts = len(records_to_scrape)

    ## It seems that the server blocks request if they are continuously sent without a pause despite using
    ## semaphore to limit requests to 10 at a time. Therefore, we only send maximum 100 requests per loop / session
    ## and sleep for 10s to have a sufficient pause.
    while current_counts > 0:  ## Keep scraping until we get all the reports, based on list of IDs
        print("Number of records to scrape =", current_counts)
        max_request_per_loop = min(100, current_counts) ## 100 or the number of records left to be scraped
        asyncio.run(async_scrape_records(collection_records, request_limit=10
                                        , url=URL_GRAHPQL, json_query=QUERY_TEMPLATE_RECORDDETAILS
                                        , record_ids=records_to_scrape[0:max_request_per_loop]))

        previous_counts = len(records_to_scrape)
        records_to_scrape = compare_records(collection_id, collection_records)
        current_counts = len(records_to_scrape)

        ## Stop scraping if the rest of the records cannot be retrieved, e.g. the server side yield empty record
        if previous_counts == current_counts:
            print("These records cannot be retrieved:\n")
            for id in records_to_scrape:
                print(f"---------------------------------\nRecord ID: {id}")
                print(list(collection_id.find(filter={'id':id})))
            break

        ## Give the server some breaks
        time.sleep(10)


if __name__ == "__main__":
    main(update=False)



    
