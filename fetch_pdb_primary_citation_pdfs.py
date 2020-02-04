from cli import pdf
import os
import requests
import pandas as pd

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
    pmids_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", filename + ".csv")
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

if __name__ == '__main__':
    pmids_file = download_pmids_from_pdbj(min_date="2020-01-01")
    pdf(pmids=None, pmidsfile=None, out=None, errors=None, maxtries=None, verbose=False)