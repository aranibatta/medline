from bs4 import BeautifulSoup
from collections import Counter
from medline.utilities.compression import extract
import datetime
import os
import re
import sqlite3
import gc
import codecs
from medline.database.db_wrapper import Database
import MySQLdb as mdb
import cPickle as pickle

from calendar import timegm
from time import gmtime, strftime, strptime, time, sleep
from dateutil import parser

from multiprocessing import Manager, Process, cpu_count

def parse_reference(ref, processing_type='basic'):

    # Load NLM data available for all references
    if processing_type == 'basic':
        data = {'pmid':             unicode(ref.pmid.text)}
    else:
        data = {'pmid':             int(ref.pmid.text),
                'pmid_version':     ref.pmid['version'],
                'ref_owner':        ref['owner'],
                'ref_status':       ref['status'],

                'journal_id': ref.medlinejournalinfo.nlmuniqueid.text,
                'journal_name': ref.medlinejournalinfo.medlineta.text}



    # Get date of last change, either daterevised or datecompleted
    if processing_type == 'complete' or processing_type == 'complete_normalized':
        if not ref.daterevised is None:
            data['date_updated'], _ = parse_date(ref.daterevised)
        else:
            data['date_updated'], _ = parse_date(ref.datecompleted)

        # Get article data
        data.update(parse_article(ref.article, processing_type))

        # Get MESH topic data
        data.update(parse_topics(ref.meshheadinglist, data['pmid']))

        # Get author information
        data.update(parse_authors(ref.authorlist, data['pmid']))

    if processing_type == 'complete_normalized':
        data.update(parse_citations(ref.commentscorrectionslist, data['pmid']))

    return data




#@profile
def parse_article(soup_article, processing_type='basic'):

    if processing_type == 'basic':
        article = {'title':     soup_article.articletitle.text}
    elif processing_type == 'complete' or processing_type == 'complete_normalized':
        article = {'title':     soup_article.articletitle.text, # 'utf8'),#.encode('utf-8'),
                  'pub_medium': soup_article['pubmodel'].lower(),
                  'pages':      parse_page_numbers(soup_article),
                  'language':       soup_article.language.text
        }

    if processing_type == 'complete' or processing_type == 'complete_normalized':
        # Get volume and issue
        try:
            article['journal_volume'] = soup_article.journal.journalissue.volume.text
        except AttributeError:
            article['journal_volume'] = None

        try:
            article['journal_issue'] = soup_article.journal.journalissue.issue.text
        except AttributeError:
            article['journal_issue'] = None

    # Set electronic and print publication dates according to publication model
    pubmodel = soup_article['pubmodel']
    if pubmodel == 'Print':
        article['date_pub_print_str'], article['date_pub_print_unix'] = parse_date(soup_article.pubdate)
        article['date_pub_first_str'] = article['date_pub_print_str']
        article['date_pub_first_unix'] = article['date_pub_print_unix']

    elif pubmodel == 'Electronic' or pubmodel == 'Electronic-eCollection':
        article['date_pub_electronic_str'], article['date_pub_electronic_unix'] = parse_date(soup_article.articledate)
        article['date_pub_first_str'] = article['date_pub_electronic_str']
        article['date_pub_first_unix'] = article['date_pub_electronic_unix']

    elif pubmodel == 'Print-Electronic' or pubmodel == 'Electronic-Print':
        article['date_pub_print_str'], article['date_pub_print_unix'] = parse_date(soup_article.pubdate)
        article['date_pub_electronic_str'], article['date_pub_electronic_unix'] = parse_date(soup_article.articledate)

        # set first publication date to the earlier one
        if article['date_pub_electronic_unix'] < article['date_pub_print_unix']:
            article['date_pub_first_str'] = article['date_pub_electronic_str']
            article['date_pub_first_unix'] = article['date_pub_electronic_unix']
        else:
            article['date_pub_first_str'] = article['date_pub_print_str']
            article['date_pub_first_unix'] = article['date_pub_print_unix']

    article['abstract'] = parse_abstract(soup_article.abstract)

    return article

def parse_citations(soup_comments, pmid):


    if soup_comments is None:
        return {'citations_list': []}

    citation_list = []

    for soup_comment in soup_comments.findAll('commentscorrections'):
        if soup_comment['reftype'] == 'Cites':
            citation_list.append({
                'citing_pmid': pmid,
                'cited_pmid': soup_comment.pmid.text
            })

    data = {'citations_list': citation_list}

    return data


PARSE_TOPIC = {'Y': 1, 'N': 0}
def parse_topics(soup_meshes, pmid):
    '''
    Parses mesh topics

    :param soup_meshes:
    :return:
    topic_list: list of all topics to insert into the normalized complete db
    major_topics and minor_topics: strings of all topics to use for the complete db.
    '''

    # If the reference contains no mesh data, return.
    if soup_meshes is None:
        return {'topics_list': [], 'topics_major' : '', 'topics_minor': ''}

    topics_list = []
    topics_major = []
    topics_minor = []
    for soup_mesh in soup_meshes.findAll('meshheading'):

        mesh = {'pmid': pmid}
        try:
            mesh['mesh_id'] = soup_mesh.descriptorname['ui']
            # 'majortopicyn' can be 'Y' or 'N' -> parse as 1 or 0
            mesh['major_topic'] = PARSE_TOPIC[soup_mesh.descriptorname['majortopicyn']]

            topic_name = soup_mesh.descriptorname.text
        except TypeError:
            # this could be used to also parse qualifier names, though that's going too far for now.
            # mesh['mesh_id'] = soup_mesh.qualifiername['ui']
            #
            # mesh['major_topic'] = PARSE_TOPIC[soup_mesh.qualifiername['majortopicyn']]
            pass


        topics_list.append(mesh)

        if mesh['major_topic'] == 1:
            topics_major.append(topic_name)
        elif mesh['major_topic'] == 0:
            topics_minor.append(topic_name)

    data = {
            'topics_list': topics_list,
            'topics_major': unicode('; '.join(topics_major)),
            'topics_minor': unicode('; '.join(topics_minor))
    }

    return data



def parse_abstract(soup_abstract):
    '''

    :param soup_abstract:
    :return:
    '''

    # if no abstract -> return None
    if soup_abstract is None:
        return None

    # Else, go through all abstracttext sections and add them to a string
    abstract = ''

    try:
        sections = soup_abstract.findAll('abstracttext')
        for section in sections:
            # get section label if exists
            try:
                label = section['label'].capitalize() + ': '
            except KeyError:
                label = ''
            abstract += label + section.text

    except AttributeError, e:
        print "Error with abstract: ", e
        print soup_abstract

    return abstract

def parse_authors(soup_authors, pmid):
    '''
    Takes a list of authers ('authorlist') and inserts them to the table refs_authors

    :param soup_authors: authorlist in xml format
    :param pmid:
    :return:
    '''

    authors_list = []
    authors_str = []
    try:
        for author in soup_authors.findAll('author'):
            last_name = author.lastname.text

            try:
                fore_name = author.forename.text
            except AttributeError:
                fore_name = None

            try:
                affiliation = author.affiliation.text
            except AttributeError:
                affiliation = None

            authors_list.append({
                'pmid': pmid,
                'last_name': last_name,
                'fore_name': fore_name,
                'affiliation': affiliation
            })
            try:
                authors_str.append(', '.join([last_name, fore_name, affiliation]))
            # affiliation or fore_name can be None -> only append last name
            except TypeError:
                authors_str.append(last_name)

        authors_str = '; '.join(authors_str)

    # if no authors, just put None.
    except AttributeError:
        authors_list = [{'pmid': pmid,
                    'last_name': None,
                    'fore_name': None,
                    'affiliation': None}]

    data = {
        'authors_list': authors_list,
        'authors_str': unicode(authors_str)
    }

    return data


MONTHS_LETTERS_DICT = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07',
                       'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
def parse_date(soup_date):
    '''
    Takes input in soup date (see below) and returns as sql compatible date string
    Input example:
    <year>2008</year>
    <month>11</month>   or <month>Nov<month>
    <day>20</day>

    Return:
    "2008-11-20"


    :param soup_date: date in xml format
    :return: sql compatible date string
    '''

    try:
        year = soup_date.year.text
    except AttributeError:
        try:
            # In rare cases, they year is stored as a range: <medlinedate>1945-1947</medlinedate>
            year = soup_date.medlinedate.text[0:4]
        except AttributeError:
            return None, None

    try:
        month = soup_date.month.text
        # if month is in 3 letter format ('Jan') parse to string ('01')
        if month in MONTHS_LETTERS_DICT:
            month = MONTHS_LETTERS_DICT[month]
    except AttributeError:
        month = '01'

    try:
        day = soup_date.day.text
    except AttributeError:
        day = '01'

    date_str = '{}-{}-{}'.format(year, month, day).encode('utf-8')

    # lifted from anser_indicus
    date_unix = int(timegm(parser.parse(date_str).timetuple()))

    return date_str, date_unix

def parse_page_numbers(soup_article):
    '''
    Parse pages from xml
    I presume that there are multiple ways of parsing it -> this is probably not yet complete.
    :param soup_pages:
    :return:
    '''

    soup_pages = soup_article.pagination

    try:
        pages = soup_pages.medlinepgn.text

    except AttributeError:
        # print "Problem parsing pages"
        # print soup_article.prettify()
        pages = None

    return pages


def get_sample_ref():


    sample_text = '''<MedlineCitationSet>
    <MedlineCitation Owner="NLM" Status="MEDLINE">
    <PMID Version="1">23797986</PMID>
    <DateCreated>
    <Year>2013</Year>
    <Month>06</Month>
    <Day>26</Day>
    </DateCreated>
    <DateCompleted>
    <Year>2014</Year>
    <Month>01</Month>
    <Day>13</Day>
    </DateCompleted>
    <DateRevised>
    <Year>2014</Year>
    <Month>11</Month>
    <Day>13</Day>
    </DateRevised>
    <Article PubModel="Print">
    <Journal>
    <ISSN IssnType="Electronic">1560-2281</ISSN>
    <JournalIssue CitedMedium="Internet">
    <Volume>19</Volume>
    <Issue>1</Issue>
    <PubDate>
    <Year>2014</Year>
    <Month>Jan</Month>
    </PubDate>
    </JournalIssue>
    <Title>Journal of biomedical optics</Title>
    <ISOAbbreviation>J Biomed Opt</ISOAbbreviation>
    </Journal>
    <ArticleTitle>High-resolution three-dimensional imaging of red blood cells parasitized by Plasmodium falciparum and in situ hemozoin crystals using optical diffraction tomography.</ArticleTitle>
    <Pagination>
    <MedlinePgn>011005</MedlinePgn>
    </Pagination>
    <ELocationID EIdType="doi" ValidYN="Y">10.1117/1.JBO.19.1.011005</ELocationID>
    <Abstract>
    <AbstractText>We present high-resolution optical tomographic images of human red blood cells (RBC) parasitized by malaria-inducing Plasmodium falciparum (Pf)-RBCs. Three-dimensional (3-D) refractive index (RI) tomograms are reconstructed by recourse to a diffraction algorithm from multiple two-dimensional holograms with various angles of illumination. These 3-D RI tomograms of Pf-RBCs show cellular and subcellular structures of host RBCs and invaded parasites in fine detail. Full asexual intraerythrocytic stages of parasite maturation (ring to trophozoite to schizont stages) are then systematically investigated using optical diffraction tomography algorithms. These analyses provide quantitative information on the structural and chemical characteristics of individual host Pf-RBCs, parasitophorous vacuole, and cytoplasm. The in situ structural evolution and chemical characteristics of subcellular hemozoin crystals are also elucidated.</AbstractText>
    </Abstract>
    <AuthorList CompleteYN="Y">
    <Author ValidYN="Y">
    <LastName>Kim</LastName>
    <ForeName>Kyoohyun</ForeName>
    <Initials>K</Initials>
    <AffiliationInfo>
    <Affiliation>Korea Advanced Institute of Science and Technology, Department of Physics, Daejeon 305-701, Republic of Korea.</Affiliation>
    </AffiliationInfo>
    </Author>
    <Author ValidYN="Y">
    <LastName>Yoon</LastName>
    <ForeName>HyeOk</ForeName>
    <Initials>H</Initials>
    </Author>
    <Author ValidYN="Y">
    <LastName>Diez-Silva</LastName>
    <ForeName>Monica</ForeName>
    <Initials>M</Initials>
    </Author>
    <Author ValidYN="Y">
    <LastName>Dao</LastName>
    <ForeName>Ming</ForeName>
    <Initials>M</Initials>
    </Author>
    <Author ValidYN="Y">
    <LastName>Dasari</LastName>
    <ForeName>Ramachandra R</ForeName>
    <Initials>RR</Initials>
    </Author>
    <Author ValidYN="Y">
    <LastName>Park</LastName>
    <ForeName>YongKeun</ForeName>
    <Initials>Y</Initials>
    </Author>
    </AuthorList>
    <Language>eng</Language>
    <GrantList CompleteYN="Y">
    <Grant>
    <GrantID>9P41-EB015871-26A1</GrantID>
    <Acronym>EB</Acronym>
    <Agency>NIBIB NIH HHS</Agency>
    <Country>United States</Country>
    </Grant>
    <Grant>
    <GrantID>P41 EB015871</GrantID>
    <Acronym>EB</Acronym>
    <Agency>NIBIB NIH HHS</Agency>
    <Country>United States</Country>
    </Grant>
    <Grant>
    <GrantID>R01HL094270</GrantID>
    <Acronym>HL</Acronym>
    <Agency>NHLBI NIH HHS</Agency>
    <Country>United States</Country>
    </Grant>
    </GrantList>
    <PublicationTypeList>
    <PublicationType UI="D016428">Journal Article</PublicationType>
    <PublicationType UI="D052061">Research Support, N.I.H., Extramural</PublicationType>
    <PublicationType UI="D013485">Research Support, Non-U.S. Gov't</PublicationType>
    <PublicationType UI="D013486">Research Support, U.S. Gov't, Non-P.H.S.</PublicationType>
    </PublicationTypeList>
    </Article>
    <MedlineJournalInfo>
    <Country>United States</Country>
    <MedlineTA>J Biomed Opt</MedlineTA>
    <NlmUniqueID>9605853</NlmUniqueID>
    <ISSNLinking>1083-3668</ISSNLinking>
    </MedlineJournalInfo>
    <ChemicalList>
    <Chemical>
    <RegistryNumber>0</RegistryNumber>
    <NameOfSubstance UI="D006420">Hemeproteins</NameOfSubstance>
    </Chemical>
    <Chemical>
    <RegistryNumber>39404-00-7</RegistryNumber>
    <NameOfSubstance UI="C020753">hemozoin</NameOfSubstance>
    </Chemical>
    </ChemicalList>
    <CitationSubset>IM</CitationSubset>
    <CommentsCorrectionsList>
    <CommentsCorrections RefType="Cites">
    <RefSource>J Theor Biol. 2010 Aug 21;265(4):493-500</RefSource>
    <PMID Version="1">20665965</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Biophys J. 2010 Aug 4;99(3):953-60</RefSource>
    <PMID Version="1">20682274</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>J Biomed Opt. 2011 Jan-Feb;16(1):011013</RefSource>
    <PMID Version="1">21280900</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>J Biomed Opt. 2011 Mar;16(3):030506</RefSource>
    <PMID Version="1">21456860</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Proc Natl Acad Sci U S A. 2011 May 3;108(18):7296-301</RefSource>
    <PMID Version="1">21504943</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Phys Med Biol. 2011 Jul 7;56(13):4013-21</RefSource>
    <PMID Version="1">21677368</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>PLoS One. 2011;6(6):e20869</RefSource>
    <PMID Version="1">21698115</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Phys Rev E Stat Nonlin Soft Matter Phys. 2011 May;83(5 Pt 1):051925</RefSource>
    <PMID Version="1">21728589</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Phys Rev Lett. 2011 Jun 10;106(23):238103</RefSource>
    <PMID Version="1">21770546</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Proc Natl Acad Sci U S A. 2011 Aug 9;108(32):13124-9</RefSource>
    <PMID Version="1">21788503</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Express. 2011 Oct 10;19(21):19907-18</RefSource>
    <PMID Version="1">21996999</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Trends Biotechnol. 2012 Feb;30(2):71-9</RefSource>
    <PMID Version="1">21930322</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>J Struct Biol. 2012 Feb;177(2):224-32</RefSource>
    <PMID Version="1">21945653</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Express. 2012 Apr 23;20(9):9673-81</RefSource>
    <PMID Version="1">22535058</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>J Biomed Opt. 2012 Apr;17(4):040501</RefSource>
    <PMID Version="1">22559667</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Sci Rep. 2012;2:614</RefSource>
    <PMID Version="1">22937223</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Phys Rev Lett. 2012 Sep 14;109(11):118105</RefSource>
    <PMID Version="1">23005682</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Acta Biomater. 2012 Nov;8(11):4130-8</RefSource>
    <PMID Version="1">22820310</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>PLoS One. 2012;7(12):e51774</RefSource>
    <PMID Version="1">23272166</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Sensors (Basel). 2013;13(4):4170-91</RefSource>
    <PMID Version="1">23539026</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Parasitol Today. 2000 Oct;16(10):427-33</RefSource>
    <PMID Version="1">11006474</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Biochem J. 2001 May 1;355(Pt 3):733-9</RefSource>
    <PMID Version="1">11311136</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>J Microsc. 2002 Feb;205(Pt 2):165-76</RefSource>
    <PMID Version="1">11879431</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Mol Biochem Parasitol. 2003 Aug 31;130(2):91-9</RefSource>
    <PMID Version="1">12946845</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Ann Trop Med Parasitol. 1978 Feb;72(1):87-8</RefSource>
    <PMID Version="1">350172</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>J Parasitol. 1979 Jun;65(3):418-20</RefSource>
    <PMID Version="1">383936</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Ultrason Imaging. 1982 Oct;4(4):336-50</RefSource>
    <PMID Version="1">6891131</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Mol Biochem Parasitol. 1990 May;40(2):269-78</RefSource>
    <PMID Version="1">2194124</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Am J Trop Med Hyg. 1990 Dec;43(6):584-96</RefSource>
    <PMID Version="1">2267961</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Biochim Biophys Acta. 1993 Jul 11;1157(3):270-4</RefSource>
    <PMID Version="1">8323956</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Proc Natl Acad Sci U S A. 1997 Jun 10;94(12):6222-7</RefSource>
    <PMID Version="1">9177198</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>J Opt Soc Am. 1957 Jun;47(6):545-56</RefSource>
    <PMID Version="1">13429433</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Lett. 2005 May 15;30(10):1162-4</RefSource>
    <PMID Version="1">15945141</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Lett. 2005 May 15;30(10):1165-7</RefSource>
    <PMID Version="1">15945142</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Lett. 2006 Mar 15;31(6):775-7</RefSource>
    <PMID Version="1">16544620</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Biochemistry. 2006 Oct 17;45(41):12400-10</RefSource>
    <PMID Version="1">17029396</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Nat Methods. 2007 Sep;4(9):717-9</RefSource>
    <PMID Version="1">17694065</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Proc Natl Acad Sci U S A. 2008 Feb 19;105(7):2463-8</RefSource>
    <PMID Version="1">18263733</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Mol Biochem Parasitol. 2008 May;159(1):7-23</RefSource>
    <PMID Version="1">18281110</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Am J Physiol Cell Physiol. 2008 Aug;295(2):C538-44</RefSource>
    <PMID Version="1">18562484</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Proc Natl Acad Sci U S A. 2008 Sep 16;105(37):13730-5</RefSource>
    <PMID Version="1">18772382</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Lett. 2008 Oct 15;33(20):2362-4</RefSource>
    <PMID Version="1">18923623</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Express. 2009 Jan 5;17(1):266-77</RefSource>
    <PMID Version="1">19129896</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Lett. 2009 Mar 1;34(5):653-5</RefSource>
    <PMID Version="1">19252582</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>PLoS Comput Biol. 2009 Apr;5(4):e1000339</RefSource>
    <PMID Version="1">19343220</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Nat Rev Microbiol. 2009 May;7(5):341-54</RefSource>
    <PMID Version="1">19369950</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Express. 2006 Aug 7;14(16):7005-13</RefSource>
    <PMID Version="1">19529071</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Express. 2009 Jul 20;17(15):12285-92</RefSource>
    <PMID Version="1">19654630</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Opt Lett. 2009 Dec 1;34(23):3668-70</RefSource>
    <PMID Version="1">19953156</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Proc Natl Acad Sci U S A. 2010 Jan 26;107(4):1289-94</RefSource>
    <PMID Version="1">20080583</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Proc Natl Acad Sci U S A. 2010 Apr 13;107(15):6731-6</RefSource>
    <PMID Version="1">20351261</PMID>
    </CommentsCorrections>
    <CommentsCorrections RefType="Cites">
    <RefSource>Cell Host Microbe. 2010 Jul 22;8(1):16-9</RefSource>
    <PMID Version="1">20638638</PMID>
    </CommentsCorrections>
    </CommentsCorrectionsList>
    <MeshHeadingList>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D000465">Algorithms</DescriptorName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D004912">Erythrocytes</DescriptorName>
    <QualifierName MajorTopicYN="Y" UI="Q000737">chemistry</QualifierName>
    <QualifierName MajorTopicYN="Y" UI="Q000469">parasitology</QualifierName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D006420">Hemeproteins</DescriptorName>
    <QualifierName MajorTopicYN="Y" UI="Q000737">chemistry</QualifierName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D006696">Holography</DescriptorName>
    <QualifierName MajorTopicYN="Y" UI="Q000379">methods</QualifierName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D006801">Humans</DescriptorName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D007091">Image Processing, Computer-Assisted</DescriptorName>
    <QualifierName MajorTopicYN="N" UI="Q000379">methods</QualifierName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D016778">Malaria, Falciparum</DescriptorName>
    <QualifierName MajorTopicYN="N" UI="Q000097">blood</QualifierName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D008853">Microscopy</DescriptorName>
    <QualifierName MajorTopicYN="Y" UI="Q000379">methods</QualifierName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D010963">Plasmodium falciparum</DescriptorName>
    <QualifierName MajorTopicYN="Y" UI="Q000737">chemistry</QualifierName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D012031">Refractometry</DescriptorName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D014054">Tomography</DescriptorName>
    <QualifierName MajorTopicYN="Y" UI="Q000379">methods</QualifierName>
    </MeshHeading>
    </MeshHeadingList>
    <OtherID Source="NLM">PMC4019420</OtherID>
    </MedlineCitation>
    </MedlineCitationSet>'''


    soup_ref = BeautifulSoup(sample_text).findAll('medlinecitation')[0]

    return soup_ref


if __name__ == "__main__":
    ref = get_sample_ref()
    p = parse_reference(ref, processing_type='complete_normalized')

    for key in p.keys():
        print key, p[key]