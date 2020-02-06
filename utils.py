# -*- coding: utf-8 -*-

"""This module contains all the constants used in pubmed2pdf repo."""

import logging
import os
import re
import urllib

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def getMainUrl(url):
    return "/".join(url.split("/")[:3])

HTML_OR_XML = re.compile('^(\s*<[!]doctype|\s*<[?]xml)', re.IGNORECASE)
JAVASCRIPT_REDIRECT = re.compile('window.location.href[ =]+"[a-zA-Z0-9$-_@.&+!*\(\), /?:%]+"')
PDF_LINK = re.compile('(type[ =]+"application/pdf"[ ]+href[ =]+"[a-zA-Z0-9$-_@.&+!*\(\), /?:%]+"|name[ =]+"citation_pdf_url"[ ]+content[ =]+"[a-zA-Z0-9$-_@.&+!*\(\), /?:%]+")')
CUSTOM_LINK = re.compile('(href="https://elifesciences.org/download/[a-zA-Z0-9$-_@.&+!*\(\), /?:%]+[.]pdf[?][a-zA-Z0-9$-_@.&+!*\(\), /?:%]+"|content[ =]+"[a-zA-Z0-9$-_@.&+!*\(\), /?:%]+"[ ]+name[ =]+"citation_pdf_url")')

def is_pdf_content(content):
    return content.lower().startswith(b'%pdf')

def savePdfFromUrl(pdf_url, output_dir, name, headers):
    if pdf_url.startswith("//"):
        pdf_url = "https:" + pdf_url
    pdf_url = pdf_url.replace("onlinelibrary.wiley.com/doi/pdf/", "onlinelibrary.wiley.com/doi/pdfdirect/")

    t = requests.get(pdf_url, headers=headers, allow_redirects=True)

    if t.status_code == 404 or t.status_code == 403:
        return False
    elif not is_pdf_content(t.content):
        decoded_content = t.content.decode('utf-8')

        if HTML_OR_XML.match(decoded_content):
            urls = JAVASCRIPT_REDIRECT.findall(decoded_content)
            pdfs = PDF_LINK.findall(decoded_content)
            custom = CUSTOM_LINK.findall(decoded_content)

            if urls:
                pdf_url = urls[0].split('"')[1]
                pdf_url = pdf_url.replace("doi/epdf/", "doi/pdfdirect/")
                pdf_url = pdf_url.replace("doi/pdf/", "doi/pdfdirect/")

                t = requests.get(pdf_url, headers=headers, allow_redirects=True)
                if not is_pdf_content(t.content):
                    return False
            elif pdfs:
                potential_url = pdfs[0].split('"')[3]

                if not "http" in potential_url:
                    fragments = pdf_url.split('/')
                    server_url = fragments[0] + "//" + fragments[2]
                    pdf_url = server_url + pdfs[0].split('"')[3]
                else:
                    pdf_url = potential_url

                t = requests.get(pdf_url, headers=headers, allow_redirects=True)
                if not is_pdf_content(t.content):
                    pdf_url = pdf_url.replace("doi/epdf/", "doi/pdfdirect/")
                    pdf_url = pdf_url.replace("doi/pdf/", "doi/pdfdirect/")

                    t = requests.get(pdf_url, headers=headers, allow_redirects=True)
                    if not is_pdf_content(t.content):
                        return False
            elif custom:
                pdf_url = custom[0].split('"')[1]

                t = requests.get(pdf_url, headers=headers, allow_redirects=True)
                if not is_pdf_content(t.content):
                    return False
            else:
                return False

    with open('{0}/{1}.pdf'.format(output_dir, name), 'wb') as f:
        for block in t.iter_content(2048):
            f.write(block)

    return True


def fetch(pmid, finders, name, headers, failed_pubmeds, output_dir):
    uri = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id={0}&retmode=ref&cmd=prlinks".format(
        pmid
    )
    success = False
    dontTry = False
    if os.path.exists("{0}/{1}.pdf".format(output_dir, pmid)):  # bypass finders if pdf reprint already stored locally
        logger.info("** Reprint #{0} already downloaded and in folder; skipping.".format(pmid))
        return
    else:
        # first, download the html from the page that is on the other side of the pubmed API
        req = requests.get(uri, headers=headers)
        if 'ovid' in req.url:
            logger.info(
                " ** Reprint {0} cannot be fetched as ovid is not supported by the requests package.".format(pmid))
            failed_pubmeds.append(pmid)
            dontTry = True
            success = True
        soup = BeautifulSoup(req.content, 'lxml')

        # loop through all finders until it finds one that return the pdf reprint
        if not dontTry:
            for finder in finders:
                logger.debug("Trying {0}".format(finder))
                pdfUrl = eval(finder)(req, soup, headers)
                if type(pdfUrl) != type(None):
                    success = savePdfFromUrl(pdfUrl, output_dir, name, headers)
                    if success:
                        logger.info("** fetching of reprint {0} succeeded".format(pmid))
                        break

        if not success:
            #hail mary
            pdfUrl = "https://www.ncbi.nlm.nih.gov/pmc/articles/pmid/{0}/".format(pmid)
            success = savePdfFromUrl(pdfUrl, output_dir, name, headers)
            if success:
                logger.info("** fetching of reprint {0} succeeded".format(pmid))

        if not success:
            logger.info("** Reprint {0} could not be fetched with the current finders.".format(pmid))
            failed_pubmeds.append(pmid)


def acsPublications(req, soup, headers):
    possibleLinks = [
        x
        for x in soup.find_all('a')
        if type(x.get('title')) == str and (
                'high-res pdf' in x.get('title').lower()
                or 'low-res pdf' in x.get('title').lower())
    ]

    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'acsPublications' finder...")
        pdfUrl = getMainUrl(req.url) + possibleLinks[0].get('href')
        return pdfUrl

    return None


def direct_pdf_link(req, soup, headers):
    if req.content[-4:] == '.pdf':
        logger.debug("** fetching reprint using the 'direct pdf link' finder...")
        pdfUrl = req.content
        return pdfUrl

    return None


def futureMedicine(req, soup, headers):
    possibleLinks = soup.find_all('a', attrs={'href': re.compile("/doi/pdf")})
    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'future medicine' finder...")
        pdfUrl = getMainUrl(req.url) + possibleLinks[0].get('href')
        return pdfUrl
    return None


def genericCitationLabelled(req, soup, headers):
    possibleLinks = soup.find_all('meta', attrs={'name': 'citation_pdf_url'})
    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'generic citation labelled' finder...")
        pdfUrl = possibleLinks[0].get('content')
        return pdfUrl
    return None


def nejm(req, soup, headers):
    possibleLinks = [
        x for x in soup.find_all('a')
        if type(x.get('data-download-type')) == str and (x.get('data-download-type').lower() == 'article pdf')
    ]

    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'NEJM' finder...")
        pdfUrl = getMainUrl(req.url) + possibleLinks[0].get('href')
        return pdfUrl

    return None


def pubmed_central_v1(req, soup, headers):
    possibleLinks = soup.find_all('a', re.compile('pdf'))

    possibleLinks = [
        x for x in possibleLinks
        if 'epdf' not in x.get('title').lower()
    ]  # this allows the pubmed_central finder to also work for wiley

    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'pubmed central' finder...")
        pdfUrl = getMainUrl(req.url) + possibleLinks[0].get('href')
        return pdfUrl

    return None


def pubmed_central_v2(req, soup, headers):
    possibleLinks = soup.find_all('a', attrs={'href': re.compile('/pmc/articles')})

    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'pubmed central' finder...")
        if "www.ncbi.nlm.nih.gov" in possibleLinks[0].get('href'):
            pdfUrl = possibleLinks[0].get('href')
        else:
            pdfUrl = "https://www.ncbi.nlm.nih.gov/{}".format(possibleLinks[0].get('href'))
        return pdfUrl

    return None

def science_direct(req, soup, headers):
    success = False

    for input in soup.find_all('input'):
        if input.get('value'):
            newUri = urllib.parse.unquote(input.get('value'))
            if "http" in newUri:
                success = True
                break

    if success:
        req = requests.get(newUri, allow_redirects=True, headers=headers)
        soup = BeautifulSoup(req.content, 'lxml')

        possibleLinks = soup.find_all('meta', attrs={'name': 'citation_pdf_url'})

        if len(possibleLinks) > 0:
            logger.debug("** fetching reprint using the 'science_direct' finder...")
            req = requests.get(possibleLinks[0].get('content'), headers=headers)
            soup = BeautifulSoup(req.content, 'lxml')
            pdfUrl = soup.find_all('a')[0].get('href')
            return pdfUrl

    return None


def cellPress(req, soup, headers):
    possibleLinks = soup.find_all('a', attrs={'href': re.compile("https://linkinghub[.]elsevier[.]com/")})

    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'cellPress' finder...")
        pdfUrl = possibleLinks[0].get('href')
        return pdfUrl
    return None


def eLife(req, soup, headers):
    possibleLinks = soup.find_all('a', attrs={'href': re.compile("/eLife[.]")})

    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'eLife' finder...")
        pdfUrl = possibleLinks[0].get('href')
        return pdfUrl
    return None


def uchicagoPress(req, soup, headers):
    possibleLinks = [
        x
        for x in soup.find_all('a')
        if type(x.get('href')) == str and 'pdf' in x.get('href') and '.edu/doi/' in x.get('href')
    ]
    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'uchicagoPress' finder...")
        pdfUrl = getMainUrl(req.url) + possibleLinks[0].get('href')
        return pdfUrl

    return None

def doiLink(req, soup, headers):
    possibleLinks = soup.find_all('a', attrs={'href': re.compile("//doi[.]org/")})

    if len(possibleLinks) > 0:
        logger.debug("** fetching reprint using the 'doiLink' finder...")
        pdfUrl = possibleLinks[0].get('href')
        return pdfUrl
    return None