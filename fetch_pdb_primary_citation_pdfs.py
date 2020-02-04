import logging
from utils import *

import os
import requests
import pandas as pd

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
PDFS_DIR = os.path.join(RESULTS_DIR, "pdfs")
ERRORS_FILE = os.path.join(RESULTS_DIR, "failed_pubmeds.csv")

def query_pdbj(sql, format, dest_file):
    pdbj_rest_url = "https://pdbj.org/rest/mine2_sql"
    params = {
        "q": sql,
        "format": format,
    }

    response = requests.get(pdbj_rest_url, params)
    response.raise_for_status()

    directory = os.path.dirname(dest_file)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(dest_file, 'wb') as handle:
        for block in response.iter_content(2048):
            handle.write(block)

def download_pmids_from_pdbj(min_date=None, max_date=None, filename="pmids", use_cache=False):
    pmids_file = os.path.join(RESULTS_DIR, filename + ".csv")
    query = """
            SELECT distinct t.pmid 
            FROM (
                SELECT a.pdbid as pdb_id, a.deposition_date as deposition_date, b."pdbx_database_id_PubMed" as pmid 
                FROM pdbj.brief_summary a left join pdbj.citation b on b.pdbid = a.pdbid
            ) as t 
        """

    if min_date is not None or max_date is not None:
        query += " WHERE"

        if min_date is not None:
            query += " deposition_date >= '{0}'".format(min_date)
        if max_date is not None:
            query += " deposition_date <= '{0}'".format(max_date)

    if not use_cache or not os.path.exists(pmids_file):
        query_pdbj(query, "csv", pmids_file)

    return pmids_file


def pdf(pmids_csv_file, output_directory=PDFS_DIR, errors_file=ERRORS_FILE, max_tries=3, verbose=False):
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
        logging.debug('Full log mode activated')
    else:
        logging.basicConfig(level=logging.INFO)

    if not os.path.exists(output_directory):
        logging.info(f"Output directory of {output_directory} did not exist.  Created the directory.")
        os.mkdir(output_directory)

    finders = [
        'genericCitationLabelled',
        'pubmed_central_v2',
        'acsPublications',
        'uchicagoPress',
        'nejm',
        'futureMedicine',
        'science_direct',
        'direct_pdf_link',
    ]

    # Add headers
    headers = requests.utils.default_headers()
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) ' \
                            'AppleWebKit/537.36 (KHTML, like Gecko) ' \
                            'Chrome/56.0.2924.87 ' \
                            'Safari/537.36'
    failed_pubmeds = []

    # Fetching pubmeds from different sources and exporting
    pmid_df = pd.read_csv(pmids_csv_file, keep_default_na=False, na_values=["", '""'], dtype='Int64')
    pmid_df = pmid_df.dropna()
    pmids = pmid_df.pmid.to_list()

    for pmid in pmids:
        logging.info("Trying to fetch pmid {0}".format(pmid))
        retriesSoFar = 0
        while retriesSoFar < max_tries:
            try:
                soup = fetch(pmid, finders, pmid, headers, failed_pubmeds, output_directory)
                retriesSoFar = max_tries
            except requests.ConnectionError as e:
                if '104' in str(e) or 'BadStatusLine' in str(e):
                    retriesSoFar += 1
                    if retriesSoFar < max_tries:
                        logging.debug("** fetching of reprint {0} failed from error {1}, retrying".format(pmid, e))
                    else:
                        logging.debug("** fetching of reprint {0} failed from error {1}".format(pmid, e))
                        failed_pubmeds.append(pmid)
                else:
                    logging.debug("** fetching of reprint {0} failed from error {1}".format(pmid, e))
                    retriesSoFar = max_tries
                    failed_pubmeds.append(pmid)
            except Exception as e:
                logging.debug("** fetching of reprint {0} failed from error {1}".format(pmid, e))
                retriesSoFar = max_tries
                failed_pubmeds.append(pmid)

    with open(errors_file, 'w+') as error_file:
        for pubmed_id in failed_pubmeds:
            error_file.write("{}\n".format(pubmed_id))


if __name__ == '__main__':
    pmids_file = download_pmids_from_pdbj(min_date="2020-01-01")
    pdf(pmids_file, verbose=True)
