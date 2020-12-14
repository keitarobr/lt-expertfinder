#
# Imports a lattes profile into the database
#
import argparse
import faulthandler
import hashlib
import io
import json
import os
import time

import mysql.connector
import pdfplumber
import psycopg2
import requests
import unidecode as unidecode
from elasticsearch import Elasticsearch
from lxml import etree
from tqdm import tqdm

faulthandler.enable()

# Models used to represent an Author and a Publication during the import process
class Author:
    def __init__(self, name, bibliography_names, publications):
        self.name = name
        self.bibliography_names = bibliography_names
        self.publications = publications

    def __str__(self):
        return f"{self.name}"

    def toJSON(self):
        return json.dumps({
            "name": self.name,
            "bibliography_names": self.bibliography_names,
            "publications": [
                pub.toJSON() for pub in self.publications
            ]
        })

class Publication:
    def __init__(self, title, co_authors, doi, year, link=None):
        self.title = title
        self.co_authors = co_authors
        self.doi = doi
        self.year = year
        self.links = []

    def hash(self):
        return hashlib.md5(self.title.encode() + self.doi.encode() + (self.co_authors[0].encode() if self.co_authors else bytes())).hexdigest()

    def __str__(self):
        return f"{self.title}"

    def toJSON(self):
        return json.dumps({
            "title": self.title,
            "co_authors": self.co_authors,
            "doi": self.doi,
            "year": self.year,
            "links": self.links
        })

# Main class - parses parameters and execute actions

class LattesImporter:
    def __init__(self):
        # Constants
        self._PATH_NOME = '/CURRICULO-VITAE/DADOS-GERAIS/@NOME-COMPLETO'
        self._PATH_NOME_PUBLICACOES = '/CURRICULO-VITAE/DADOS-GERAIS/@NOME-EM-CITACOES-BIBLIOGRAFICAS'

        self._PATH_PUBLICACOES_EVENTOS = '/CURRICULO-VITAE/PRODUCAO-BIBLIOGRAFICA/TRABALHOS-EM-EVENTOS/TRABALHO-EM-EVENTOS/DADOS-BASICOS-DO-TRABALHO/@TITULO-DO-TRABALHO'
        self._PATH_DOI_EVENTO = '/CURRICULO-VITAE/PRODUCAO-BIBLIOGRAFICA/TRABALHOS-EM-EVENTOS/TRABALHO-EM-EVENTOS/DADOS-BASICOS-DO-TRABALHO[@TITULO-DO-TRABALHO="{title}"]/parent::TRABALHO-EM-EVENTOS/DADOS-BASICOS-DO-TRABALHO/@DOI'
        self._PATH_ANO_EVENTO = '/CURRICULO-VITAE/PRODUCAO-BIBLIOGRAFICA/TRABALHOS-EM-EVENTOS/TRABALHO-EM-EVENTOS/DADOS-BASICOS-DO-TRABALHO[@TITULO-DO-TRABALHO="{title}"]/parent::TRABALHO-EM-EVENTOS/DADOS-BASICOS-DO-TRABALHO/@ANO-DO-TRABALHO'
        self._PATH_COAUTORES_EVENTO = '/CURRICULO-VITAE/PRODUCAO-BIBLIOGRAFICA/TRABALHOS-EM-EVENTOS/TRABALHO-EM-EVENTOS/DADOS-BASICOS-DO-TRABALHO[@TITULO-DO-TRABALHO="{title}"]/parent::TRABALHO-EM-EVENTOS/AUTORES/@NOME-COMPLETO-DO-AUTOR'

        self._PATH_PUBLICACOES_ARTIGOS = '/CURRICULO-VITAE/PRODUCAO-BIBLIOGRAFICA/ARTIGOS-PUBLICADOS/ARTIGO-PUBLICADO/DADOS-BASICOS-DO-ARTIGO/@TITULO-DO-ARTIGO'
        self._PATH_DOI_ARTIGO = '/CURRICULO-VITAE/PRODUCAO-BIBLIOGRAFICA/ARTIGOS-PUBLICADOS/ARTIGO-PUBLICADO/DADOS-BASICOS-DO-ARTIGO[@TITULO-DO-ARTIGO="{title}"]/parent::ARTIGO-PUBLICADO/DADOS-BASICOS-DO-ARTIGO/@DOI'
        self._PATH_ANO_ARTIGO = '/CURRICULO-VITAE/PRODUCAO-BIBLIOGRAFICA/ARTIGOS-PUBLICADOS/ARTIGO-PUBLICADO/DADOS-BASICOS-DO-ARTIGO[@TITULO-DO-ARTIGO="{title}"]/parent::ARTIGO-PUBLICADO/DADOS-BASICOS-DO-ARTIGO/@ANO-DO-ARTIGO'
        self._PATH_COAUTORES_ARTIGO = '/CURRICULO-VITAE/PRODUCAO-BIBLIOGRAFICA/ARTIGOS-PUBLICADOS/ARTIGO-PUBLICADO/DADOS-BASICOS-DO-ARTIGO[@TITULO-DO-ARTIGO="{title}"]/parent::ARTIGO-PUBLICADO/AUTORES/@NOME-COMPLETO-DO-AUTOR'

        self._SQL_FIND_BY_TYTLE = '''
                select
                    p.id
                from publication p
                    inner join publication_title pt on (pt.publication_id=p.id)
                where
                    pt.title = %s
            '''

        self._SQL_FIND_BY_TITLE_SIMILARITY = '''
        select
            p.id
        from publication p
        	inner join publication_title pt on (pt.publication_id=p.id)
        where
        	pt.title %% %s
        ORDER BY
            similarity(pt.title, %s) DESC
        LIMIT 5
        '''

        self._SQL_FIND_AUTHORS = '''
        select
            a.name
        from publication_author pa 
            inner join author a on (pa.author_id=a.id)    
        where
            pa.publication_id=%s
        '''

        self._SQL_FIND_LINKS = '''
        select
            pl.link
        from publication_link pl 
        where
            pl.publication_id=%s
        '''

        self._SQL_FIND_PUBLICATION_CACHE = 'SELECT links FROM publication_cache WHERE hash = %s'
        self._SQL_ADD_PUBLICATION_CACHE = 'INSERT INTO publication_cache (hash, details, links) values (%s, %s, %s)'

        self._SQL_FIND_DOCUMENT_CACHE = 'SELECT body FROM document_cache WHERE hash = %s'
        self._SQL_ADD_DOCUMENT_CACHE = 'INSERT INTO document_cache (hash, link, body) VALUES (%s, %s, %s)'

        self._SQL_ADD_AUTHOR = "SELECT add_authorAAN(%s, %s)"
        self._SQL_ADD_DOCUMENT_AAN = "SELECT add_documentAAN(%s, %s, %s, %s)"
        self._SQL_ADD_PUBLICATION_AAN = "SELECT add_publicationAAN(%s, %s)"

    def _build_args_parser(self):
        parser = argparse.ArgumentParser(description='Lattes importer.')
        parser.add_argument('action',
                            choices=['import_profile'],
                            help='action to execute')
        parser.add_argument('--profile_xml_file_path', nargs='?',
                            help='path of the file with the lattes profile')
        parser.add_argument('--crossrefhost', nargs='?',
                            help='crossref postgres database host')
        parser.add_argument('--crossrefuser', nargs='?',
                            help='crossref database user')
        parser.add_argument('--crossrefpass', nargs='?',
                            help='crossref database password')
        parser.add_argument('--crossrefdb', nargs='?',
                            help='crossref database')

        parser.add_argument('--xpertfinderhost', nargs='?',
                            help='xpertfinder mysql database host')
        parser.add_argument('--xpertfinderuser', nargs='?',
                            help='xpertfinder database user')
        parser.add_argument('--xpertfinderpass', nargs='?',
                            help='xpertfinder database password')
        parser.add_argument('--xpertfinderdb', nargs='?',
                            help='xpertfinder database')

        parser.add_argument('--lchost', nargs='?',
                            help='lattes_cache mysql database host')
        parser.add_argument('--lcuser', nargs='?',
                            help='lattes_cache database user')
        parser.add_argument('--lcpass', nargs='?',
                            help='lattes_cache database password')
        parser.add_argument('--lcdb', nargs='?',
                            help='lattes_cache database')

        parser.add_argument('--elhost', nargs='?',
                            help='elastic search host')


        return parser

    def _validate_args(self, args):
        if args.action == 'import_profile' and (
                args.profile_xml_file_path is None or args.crossrefhost is None or args.xpertfinderhost is None
        or args.lchost is None or args.elhost is None):
            print(
                "For [import_profile] parameters --profile_xml_file_path, --xpertfinderhost, --crossrefhost, --lchost and --elhost are required")
            return False
        return True

    def _get_crossref_connection(self):
        return psycopg2.connect(user=self._args.crossrefuser if self._args.crossrefuser else 'crossref',
                          password=self._args.crossrefpass if self._args.crossrefpass else 'crossref',
                          host=self._args.crossrefhost,
                          port='5432',
                          database=self._args.crossrefdb if self._args.crossrefdb else 'crossref')

    def _get_lattes_cache_connection(self):
        return psycopg2.connect(user=self._args.lcfuser if self._args.lcuser else 'lattes_cache',
                          password=self._args.lcpass if self._args.lcpass else 'lattes_cache',
                          host=self._args.lchost,
                          port='5432',
                          database=self._args.lcdb if self._args.lcdb else 'lattes_cache')

    def _get_xpertfinder_connection(self):
        return mysql.connector.connect(
          host=self._args.xpertfinderhost,
          user=self._args.xpertfinderuser if self._args.xpertfinderuser else "xpertfinder",
          passwd=self._args.xpertfinderpass if self._args.xpertfinderpass else "xpertfinder",
          database=self._args.xpertfinderdb if self._args.xpertfinderdb else "xpertfinder"
        )

    def run(self):
        parser = self._build_args_parser()
        args = parser.parse_args()
        if self._validate_args(args):
            self._args = args
            # at the moment, single action - import profile
            if not os.path.exists(args.profile_xml_file_path):
                print("Profile file not found!")
            else:
                self.import_profile(args.profile_xml_file_path)

    def import_profile(self, profile_xml_path):
        # filename example: 5083038168307301.xml
        author_id = profile_xml_path.split('/')[-1].split('.')[-2]
        print(f"Importing author with id {author_id}")
        with open(profile_xml_path, "rb") as f:
            self._profile_xml = etree.parse(f)
        self._load_names()
        print(f"Names associated with the author: {self._names_search}")

        print(f"Loading author publications")
        self._get_publications()

        print(f"Finding publication links")
        self._load_links()
        self._author = Author(name=self._author_name, bibliography_names=self._names_search, publications=self._publications)

        print(f"Downloading documents for the author")
        self._download_docs()

        print(f"Creating author in xpertfinder")
        self._add_author()

        print(f"Creating elasticsearch index")
        self._create_elastic_cache()

        print(f"Importing documents into elasticsearch index")
        self._import_docs()

        print(f"Creating documents in xpertfinder")
        for pub in tqdm(iterable=self._author.publications, desc='Adding documents'):
            self._add_document(f"{pub.hash()}", f"{pub.title}", "", f"{pub.year}")
            self._add_publication(self._author.name, pub.hash())


    def _add_document(self, file, title, venue, year):
        params = (file.strip(), unidecode.unidecode(title), venue.strip(), year)
        xpert_conn = self._get_xpertfinder_connection()
        cursor = xpert_conn.cursor()
        cursor.execute(self._SQL_ADD_DOCUMENT_AAN, params)
        data = cursor.fetchall()
        xpert_conn.commit()
        xpert_conn.close()

    def _add_publication(self, author, document):
        params = (author.lower().strip(), document.strip())
        xpert_conn = self._get_xpertfinder_connection()
        cursor = xpert_conn.cursor()
        cursor.execute(self._SQL_ADD_PUBLICATION_AAN, params)
        data = cursor.fetchall()
        xpert_conn.commit()
        xpert_conn.close()

    def _download_docs(self):
        lc_connection = self._get_lattes_cache_connection()

        for doc in tqdm(iterable=[doc for doc in self._author.publications if doc.links], desc='Downloading documents'):
            cursor = lc_connection.cursor()
            cursor.execute(self._SQL_FIND_DOCUMENT_CACHE, (doc.hash(),))
            doc_data = cursor.fetchall()
            cursor.close()

            if not doc_data:
                links_pdf = [link for link in doc.links if "pdf" in link]
                links_download = [link for link in doc.links if "download" in link]
                links_dwn = [link for link in doc.links if "dwn" in link]
                links = links_pdf + links_download + links_dwn

                for link_pdf in links:
                    try:
                        pdf = requests.get(link_pdf, allow_redirects=True)
                        if 'pdf' in pdf.headers.get('content-type').lower():
                            cursor = lc_connection.cursor()
                            cursor.execute(self._SQL_ADD_DOCUMENT_CACHE, (doc.hash(), link_pdf, pdf.content))
                            cursor.close()
                            lc_connection.commit()
                            break
                    except:
                        pass

        lc_connection.close()

    def _add_author(self):
        splitted = self._author_name.split(", ")
        altname = ""
        if len(splitted) > 1:
            altname = splitted[1].lower().strip() + " " + splitted[0].lower().strip()

        params = (self._author_name.lower().strip(), altname.lower().strip())
        con_xpertfinder = self._get_xpertfinder_connection()
        cursor = con_xpertfinder.cursor()
        cursor.execute(self._SQL_ADD_AUTHOR, params)
        cursor.fetchall()
        con_xpertfinder.commit()
        con_xpertfinder.close()

    def _load_links(self):

        lc_connection = self._get_lattes_cache_connection()
        for pub in tqdm(iterable=self._publications, desc='Profile import'):
            cursor = lc_connection.cursor()
            cursor.execute(self._SQL_FIND_PUBLICATION_CACHE, (pub.hash(), ))
            pub_data = cursor.fetchall()
            cursor.close()

            if pub_data:
                pub.links = pub_data[0][0].split('\n') if pub_data[0][0] else []
            else:
                pub.links = self._find_links_publication(pub)
        lc_connection.close()

    def _find_links_publication(self, pub):

        cr_connection = self._get_crossref_connection()
        lc_connection = self._get_lattes_cache_connection()

        self._start_timer()
        cursor = cr_connection.cursor()
        cursor.execute(self._SQL_FIND_BY_TYTLE, (pub.title,))
        docs = cursor.fetchall()
        cursor.close()
        self._end_timer("Seconds for direct search: ")

        if len(docs) == 0:
            cursor = cr_connection.cursor()
            self._start_timer()
            cursor.execute(self._SQL_FIND_BY_TITLE_SIMILARITY, (pub.title, pub.title))
            docs = cursor.fetchall()
            self._end_timer("Seconds for similarity search: ")

        urls = []
        if len(docs) > 0:
            for doc in docs:
                cursor = cr_connection.cursor()
                cursor.execute(self._SQL_FIND_AUTHORS, (doc[0],))
                authors = cursor.fetchall()
                authors = [author[0] for author in authors]
                cursor.close()

                if len(authors) > 0:
                    is_author = any(author.upper() in self._names_search for author in authors)

                    if is_author:
                        cursor = cr_connection.cursor()
                        cursor.execute(self._SQL_FIND_LINKS, (doc[0],))
                        links = cursor.fetchall()
                        cursor.close()

                        for link in links:
                            urls.append(link[0])

        cursor = lc_connection.cursor()
        cursor.execute(self._SQL_ADD_PUBLICATION_CACHE, (pub.hash(), pub.toJSON(), '\n'.join(urls)))
        cursor.close()
        lc_connection.commit()
        lc_connection.close()
        cr_connection.close()

    def _load_names(self):
        self._author_name = self._profile_xml.xpath(self._PATH_NOME)[0]
        author_name_publications = self._profile_xml.xpath(self._PATH_NOME_PUBLICACOES)[0]
        if author_name_publications:
            self._names_search = author_name_publications.split(';')
        else:
            self._names_search = [self._author_name]


        publications_names_upper = [name.upper() for name in self._names_search]
        publications_names = [name for name in publications_names_upper]
        for name in publications_names_upper:
            name_parts = name.split(',')
            name_parts = [name.strip() for name in name_parts]
            if len(name_parts) > 1:
                publications_names.append(", ".join(name_parts[::-1]))
                publications_names.append(" ".join(name_parts[::-1]))
                publications_names.append(" ".join(name_parts))

        self._names_search = publications_names

    def _get_publications(self):
        self._publications = self._get_event_publications() + self._get_articles()

    def _get_articles(self):
        publications = self._profile_xml.xpath(self._PATH_PUBLICACOES_ARTIGOS)
        result = []

        for pub in publications:
            pub = pub.replace('"', '\'')
            path_co_authors = self._PATH_COAUTORES_ARTIGO.format(title=pub)
            co_authors = self._profile_xml.xpath(path_co_authors)

            path_doi = self._PATH_DOI_ARTIGO.format(title=pub)
            doi = self._profile_xml.xpath(path_doi)

            if not doi or not doi[0]:
                doi = ''
            else:
                doi = doi[0]

            path_year = self._PATH_ANO_ARTIGO.format(title=pub)
            year = self._profile_xml.xpath(path_year)
            if not year or not year[0]:
                year = 0
            else:
                year = year[0]

            result.append(Publication(title=pub, co_authors=co_authors, year=year, doi=doi))
        return result

    def _get_event_publications(self):
        pubs = self._profile_xml.xpath(self._PATH_PUBLICACOES_EVENTOS)

        result = []
        for pub in pubs:
            pub = pub.replace('"', '\'')
            path_coauthors = self._PATH_COAUTORES_EVENTO.format(title=pub)
            co_authors = self._profile_xml.xpath(path_coauthors)

            path_doi = self._PATH_DOI_EVENTO.format(title=pub)
            doi = self._profile_xml.xpath(path_doi)

            if not doi or not doi[0]:
                doi = ''
            else:
                doi = doi[0]

            path_year = self._PATH_ANO_EVENTO.format(title=pub)
            year = self._profile_xml.xpath(path_year)
            if not year or not year[0]:
                year = 0
            else:
                year = year[0]

            result.append(Publication(title=pub, co_authors=co_authors, year=year, doi=doi))

        return result

    def _create_elastic_cache(self):
        es = Elasticsearch([self._args.elhost], port=9200)
        index = "aan"
        mapping = {"mappings": {
            "properties": {
                "text": {
                    "type": "text",
                    "fields": {
                        "length": {
                            "type": "token_count",
                            "analyzer": "standard",
                            "store": "true"
                        }
                    },
                    "term_vector": "yes",
                    "store": True,
                    "analyzer": "fulltext_analyzer"
                },
                "doc": {
                    "properties": {
                        "text": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                },
                "field_statistics": {
                    "type": "boolean"
                },
                "fields": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "offsets": {
                    "type": "boolean"
                },
                "positions": {
                    "type": "boolean"
                },
                "term_statistics": {
                    "type": "boolean"
                }
            }
        },
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "analysis": {
                    "analyzer": {
                        "fulltext_analyzer": {
                            "type": "custom",
                            "tokenizer": "whitespace",
                            "filter": [
                                "lowercase",
                                "type_as_payload"
                            ]
                        }
                    }
                }
            }
        }

        if not es.indices.exists(index):
            print("creating '%s' index..." % index)
            res = es.indices.create(index=index, body=mapping)
            print(" response: '%s'" % res)

    def _import_docs(self):
        lc_connection = self._get_lattes_cache_connection()
        es = Elasticsearch([self._args.elhost], port=9200)
        index = "aan"
        timeout = 30

        for doc in tqdm(iterable=[doc for doc in self._author.publications if doc.links], desc='Importing documents'):
            cursor = lc_connection.cursor()
            cursor.execute(self._SQL_FIND_DOCUMENT_CACHE, (doc.hash(),))
            doc_body = cursor.fetchall()
            cursor.close()

            if doc_body:  # and not article_cache.get(doc.hash() + '-els'):
                id = doc.hash()
                data_obj = {'text': self._extract_text(doc_body[0][0])}
                json_data = json.dumps(data_obj)
                op_dict = {
                    "index": {
                        "_index": index,
                        "_type": "_doc",
                        "_id": id
                    }
                }
                bulk_data = []
                bulk_data.append(op_dict)
                bulk_data.append(json_data)
                res = es.bulk(index=index, body=bulk_data, refresh=True, request_timeout=timeout)
                if res['errors']:
                    print(res)

    def _extract_text(self, pdf):
        pdfReader = pdfplumber.open(io.BytesIO(pdf))
        text = ""
        for page in pdfReader.pages:
            text = text + page.extract_text() + "\n"
        return text

    def _start_timer(self):
        self._start_time = time.time()

    def _end_timer(self, msg):
        end = time.time()
        print(msg + " => " + str(end - self._start_time))


LattesImporter().run()