# -*- coding: utf-8 -*-
import sqlite3
import geopy
import re
import time
from collections import Counter

locator = geopy.geocoders.Nominatim()

university_regex = re.compile(r"[^,;]*(University|College|Institute)[^,;]*[,;][^,;]*")

hospital_regex = re.compile(u"[^,;]*(Hospital|Medical Center|Hôpital)[^,;]*[,;][^,;]*")

UNIVERSITIES = [

    # Alaska
    ('University of Alaska',                    'university of alaska, fairbanks',          ['"university of alaska"']),

    # Arkansas
    ('University of Arkansas',                  (36.0687, -94.1760),        ['"university of arkansas"']),

    # Arizona
    ('Arizona State University',                'arizona state university, tempe',                     ['"arizona state university"']),
    ('Northern Arizona University',             'northern arizona university',                         ['"northern arizona university"']),
    ('University of Arizona',                   'university of arizona, tucson',                       ['"university of arizona"']),

    # Alabama
    ('Auburn University',                       (32.6033, -85.4860),        ['"auburn university"']),
    ('University of Alabama, Birmingham',       (33.5012, -86.8053),        ['"university of alabama" birmingham']),
    ('University of Alabama, Huntsville',       (34.7271, -86.6398),        ['"university of alabama" huntsville']),
    ('University of Alabama, Tuscaloosa',       (33.2094, -87.5414),        ['"university of alabama", tuscaloosa']),
    ('University of South Alabama',             (30.6967, -88.1787),        ['"university of south alabama"']),

    # California
    ('California State University',             '[university]california state university',             ['"california state university"']),
    ('University of California, Berkeley',      '[university]University of California, Berkeley',      ['"university of california, berkeley"', '"uc berkeley"']),
    ('University of California, Davis',         '[university]University of California, Davis',         ['"university of california, davis"', '"uc davis"']),
    ('University of California, Irvine',        '[university]University of California, Irvine',        ['"university of california, irvine"', '"uc irvine"']),
    ('University of California, Los Angeles',   '[university]University of California, Los Angeles',   ['"university of california, los angeles"', 'ucla']),
    ('University of California, Merced',        '[university]University of California, Merced',        ['"university of california, merced"', '"uc merced"']),
    ('University of California, Riverside',     '[university]University of California, Riverside',     ['"university of california, riverside"', '"uc riverside"']),
    ('University of California, San Diego',     '[university]University of California, San Diego',     ['"university of california, san diego"', '"uc san diego"']),
    ('University of California, San Francisco', '[university]University of California, San Francisco', ['"university of california, san francisco"', 'ucsf']),
    ('University of California, Santa Barbara', '[university]University of California, Santa Barbara', ['"university of california, santa barbara"', '"uc santa barbara"']),
    ('University of California, Santa Cruz',    '[university]University of California, Santa Cruz',    ['"university of california, santa cruz"', 'ucsc']),

    ('California Institute of Technology',      'California Institute of Technology',                  ['"California Institute of Technology"']),
    ('Claremont Graduate University',           'claremont graduate university',                       ['"claremont graduate university"']),
    ('Stanford University',                     'Stanford University',                                 ['"stanford university"']),
    ('University of Southern California',       'University of Southern California',                   ['"university of southern california"']),

    # Colorado
    ('Colorado School of Mines',                'colorado school of mines',                            ['"colorado school of mines"']),
    ('Colorado State University',               'colorado state university, fort',                     ['"colorado state university"']),
    ('University of Colorado',                  '[university]university of colorado, boulder',         ['"university of colorado"']),
    ('University of Denver',                    'university of denver',                                ['"university of denver"']),

    # Connecticut
    ('University of Connecticut',               (41.8071, -72.2525),        ['"university of connecticut"']),
    ('Yale University',                         (41.3111, -72.9267),        ['"yale university"']),

    # Delaware
    ('University of Delaware',                  (39.6791, -75.7521),        ['"university of delaware"']),

    # Florida
    ('Florida Atlantic University',             (26.3712, -80.1016),        ['"florida atlantic university"']),
    ('Florida International University',        (25.7574, -80.3732),        ['"florida international university"']),
    ('Florida State University',                (30.4420, -84.2980),        ['"florida state university"']),
    ('Nova Southeastern University',            (26.0779, -80.2418),        ['"nova southeastern university"']),
    ('University of Central Florida',           (28.6016, -81.2005),        ['"university of central florida"']),
    ('University of Florida',                   (29.6483, -82.3494),        ['"university of florida"']),
    ('University of Miami',                     (25.7216, -80.2792),        ['"university of miami"']),
    ('University of South Florida',             (28.0545, -82.4130),        ['"university of south florida"']),

    # Georgia
    ('Emory University',                        (33.7911, -84.3233),        ['"emory university"']),
    ('Georgia Institute of Technology',         (33.7758, -84.3947),        ['"georgia institute of technology"']),
    ('Georgia State University',                (33.7527, -84.3861),        ['"georgia state university"']),
    ('University of Georgia',                   (33.9558, -83.3745),        ['"university of georgia"']),

    # Hawaii
    ('University of Hawaii',                    'university of hawaii',                     ['"university of hawai''i"', '"university of hawaii"']),

    # Iowa
    ('Iowa State University',                   (42.0239, -93.6476),                        ['"iowa state university"']),
    ('University of Iowa',                      (41.65,   -91.5333),                        ['"university of iowa"']),

    # Idaho
    ('Boise State University',                  'boise state university',                   ['"boise state university"']),
    ('Idaho State University',                  'idaho state university',                   ['"idaho state university"']),
    ('University of Idaho',                     'university of idaho',                      ['"university of idaho"']),

    # Illinois
    ('Illinois Institute of Technology',        (41.8347, -87.6283),        ['"illinois institute of technology"']),
    ('Loyola University',                       (41.9999, -87.6578),        ['"loyola university"']),
    ('Northern Illinois University',            (41.9339, -88.7778),        ['"northern illinois university"']),
    ('Northwestern University',                 (42.0548, -87.6739),        ['"Northwestern University"']),
    ('Southern Illinois University',            (37.7104, -89.2193),        ['"southern illinois university"']),
    ('University of Chicago',                   (41.7897, -87.5997),        ['"university of chicago"']),
    ('University of Illinois, Urbana-Champaign',(40.1105, -88.2284),        ['"university of illinois" urbana']),
    ('University of Illinois, Chicago',         (41.8719, -87.6493),        ['"university of illinois" chicago']),

    # Indiana
    ('Ball State University',                   (40.1983, -85.4089),        ['"ball state university"']),
    ('Indiana University',                      (39.1772, -86.5154),        ['"indiana university"']),
    ('Purdue University',                       (40.4240, -86.9290),        ['"purdue university"']),
    ('University of Notre Dame',                (41.7029, -86.2389),        ['"university of notre dame"']),


    # Kansas
    ('Kansas State University',                 'kansas state university',                  ['"kansas state university"']),
    ('Wichita State University',                'wichita state university',                 ['"wichita state university"']),
    ('University of Kansas',                    'university of kansas',                     ['"university of kansas"']),

    # Kentucky
    ('University of Kentucky',                  (38.0333, -84.5000),        ['"university of kentucky"']),
    ('University of Louisville',                (38.2150, -85.7602),        ['"university of louisville"']),

    # Louisiana
    ('Louisiana State University',              (30.4145, -91.1770),        ['"louisiana state university"']),
    ('Louisiana Tech University',               (32.5273, -92.6470),        ['"louisiana tech university"']),
    ('Tulane University',                       (29.9353, -90.1227),        ['"tulane university"']),
    ('University of Louisiana',                 (30.2126, -92.0193),        ['"university of louisiana"']),
    ('University of New Orleans',               (30.0275, -90.0671),        ['"university of new orleans"']),

    # Maine
    ('University of Maine',                     (44.8880, -68.6717),        ['"university of maine"']),

    # Maryland
    ('Johns Hopkins University',                (39.3288, -76.6202),        ['"johns hopkins university"']),
    ('University of Maryland, College Park',    (38.9875, -76.9400),        ['"university of maryland" college']),
    ('University of Maryland, Baltimore County',(39.2555, -76.7112),        ['"university of maryland" baltimore']),

    # Massachusetts
    ('Boston College',                          (42.3350, -71.1703),        ['boston college']),
    ('Boston University',                       (42.3496, -71.0997),        ['"boston university"']),
    ('Brandeis University',                     (42.3656, -71.2597),        ['"brandeis university"']),
    ('Brown University',                        (41.8262, -71.4032),        ['"brown university"']),
    ('Clark University',                        (42.2509, -71.8231),        ['"clark university"']),
    ('Harvard University',                      (42.3744, -71.1169),        ['"harvard university"', '"harvard medical"']),
    ('Massachusetts Institute of Technology',   (42.3598, -71.0921),        ['"massachusetts institute of technology"']),
    ('Northeastern University',                 (42.3383, -71.0879),        ['"northeastern university"']),
    ('Tufts University',                        (42.4060, -71.1200),        ['"tufts university"']),
    ('University of Massachusetts',             (42.3888, -72.5277),        ['"university of massachusetts"']),

    # Michigan
    ('Michigan State University',               (42.723,  -84.481 ),        ['"michigan state university"']),
    ('Michigan Technological University',       (47.12,   -88.55  ),        ['"Michigan Technological University"']),
    ('University of Michigan',                  (42.2942, -83.7100),        ['"university of michigan"']),
    ('Wayne State University',                  (42.3573, -83.0701),        ['"Wayne State University"']),
    ('Western Michigan University',             (42.2833, -85.6139),        ['"western michigan university"']),

    # Minnesota
    ('University of Minnesota',                 (44.9747, -93.2353),        ['"university of minnesota"']),

    # Mississippi
    ('Jackson State University',                (32.2961, -90.2077),        ['"jackson state university"']),
    ('Mississippi State University',            (33.4540, -88.7890),        ['"mississippi state university"']),
    ('University of Mississippi',               (34.3650, -89.5380),        ['"university of mississippi"']),
    ('University of Southern Mississippi',      (31.3296, -89.3338),        ['"university of southern mississippi"']),

    # Missouri
    ('Missouri University of Science and Technology', (37.9555, -91.7735),  ['"missouri university of science and technology"']),
    ('Saint Louis University',                  (38.6365, -90.2339),        ['"saint louis university"']),
    ('Washington University in St. Louis',      (38.648,  -90.305),         ['"washington university" st. louis']),
    ('University of Missouri, Columbia',        (38.9453, -92.3288),        ['"university of missouri" columbia']),
    ('University of Missouri, Kansas City',     (39.0335, -94.5756),        ['"university of missouri" kansas city']),
    ('University of Missouri, St. Louis',       (38.7102, -90.3110),        ['"university of missouri" st. louis']),

    # Montana
    ('Montana State University',                'montana state university',                 ['"montana state university"']),
    ('University of Montana',                   'university of montana',                    ['"university of montana"']),

    # Nebraska
    ('University of Nebraska',                  'university of nebraska, lincoln',          ['"university of nebraska"']),

    # Nevada
    ('University of Nevada, Las Vegas',         'university of nevada, las vegas',          ['university nevada las vegas']),
    ('University of Nevada, Reno',              'university of nevada, reno',               ['university nevada reno']),

    # New Hampshire
    ('Dartmouth College',                       (43.7033, -72.2883),        ['"dartmouth college"']),
    ('University of New Hampshire',             (43.1355, -70.9333),        ['"university of new hampshire"']),

    # New Jersey
    ('New Jersey Institute of Technology',      (40.7422, -74.1785),        ['"new jersey institute of technology"']),
    ('Princeton University',                    (40.3487, -74.6593),        ['"princeton university"']),
    ('Rutgers University',                      (40.5018, -74.4481),        ['"rutgers university"']),
    ('Stevens Institute of Technology',         (40.7449, -74.0239),        ['"stevens institute of technology"']),

    # New Mexico
    ('New Mexico State University',             'new mexico state university',              ['"new mexico state university"']),
    ('University of New Mexico',                'university of new mexico',                 ['"university of new mexico"']),

    # New York
    ('City University of New York',             (40.7483, -73.9833),        ['"city university" "new york"', 'cuny']),
    ('Columbia University',                     (40.8075, -73.9619),        ['"columbia university"']),
    ('Cornell University',                      (42.4485, -76.4786),        ['"cornell university"']),
    ('New York University',                     (40.7300, -73.9950),        ['"new york university"']),
    ('Rensselaer Polytechnic Institute',        (42.7300, -73.6775),        ['"rensselaer polytechnic institute"']),
    ('Rockefeller University',                  (40.7625, -73.9555),        ['"rockefeller university"']),
    ('SUNY Albany',                             (42.6861, -73.8238),        ['"university at albany"', '"state university" albany', '"albany medical college"']),
    ('SUNY Binghamton',                         (42.0888, -75.9670),        ['"state university" binghamton', '"binghamton university"']),
    ('SUNY Buffalo',                            (43.0000, -78.7900),        ['"state university" buffalo', '"university at buffalo"']),
    ('SUNY Stony Brook',                        (40.9142, -73.1162),        ['"state university" stony brook', '"stony brook university"']),
    ('University of Rochester',                 (43.1283, -77.6283),        ['"university of rochester"']),
    ('Yeshiva University',                      (40.8502, -73.9297),        ['"yeshiva university"']),

    # North Carolina
    ('Duke University',                         (36.0011, -78.9388),        ['"duke university"']),
    ('North Carolina State University',         (35.7860, -78.6820),        ['"north carolina state university"']),
    ('University of North Carolina, Greensboro',(36.0695, -79.8114),        ['"university of north carolina" greensboro']),
    ('University of North Carolina, Chapel Hill',(35.9083, -79.050),        ['"university of north carolina" chapel']),
    ('Wake Forest University',                  (36.1350, -80.2770),        ['"wake forest university"']),

    # North Dakota
    ('North Dakota State University',           'north dakota state university',            ['"north dakota state university"']),
    ('University of North Dakota',              'university of north dakota',               ['"university of north dakota"']),

    # Ohio
    ('Bowling Green State University',          (41.3800, -83.6400),        ['"bowling green state university"']),
    ('Case Western Reserve University',         (41.5041, -81.6084),        ['"case western reserve"']),
    ('Cleveland State University',              (41.5017, -81.6751),        ['"cleveland state university"']),
    ('Kent State University',                   (41.1464, -81.3417),        ['"kent state university"']),
    ('Miami University, Ohio',                  (39.5119, -84.7346),        ['"miami university"']),
    ('Ohio State University',                   (40.0000, -83.0145),        ['"ohio state university"']),
    ('Ohio University',                         (39.3231, -82.0954),        ['"ohio university"']),
    ('University of Akron',                     (41.0752, -81.5115),        ['"university of akron"']),
    ('University of Cincinnati',                (39.1320, -84.5155),        ['"university of cincinnati"']),
    ('University of Dayton',                    (39.7404, -84.1792),        ['"university of dayton"']),
    ('University of Toledo',                    (41.6577, -83.6136),        ['"university of toledo"']),
    ('Wright State University',                 (39.7798, -84.0647),        ['"wright state university"']),

    # Oklahoma
    ('Oklahoma State University',               'oklahoma state university',                ['"oklahoma state university"']),
    ('University of Oklahoma',                  'university of oklahoma',                   ['"university of oklahoma"']),

    # Oregon
    ('Oregon State University',                 '[university]oregon state university, corvallis',      ['"oregon state university"']),
    ('Oregon Health & Science University',      'oregon health & science university',                  ['"oregon health science university"', '"oregon health sciences university"']),
    ('Portland State University',               'portland statue university',                          ['"portland state university"']),
    ('University of Oregon',                    'university of oregon',                                ['"university of oregon"']),

    # Pennsylvania
    ('Carnegie Mellon University',              (40.4433, -79.9435),        ['"carnegie mellon university"']),
    ('Drexel University',                       (39.9540, -75.1880),        ['"drexel university"']),
    ('Duquesne University',                     (40.4361, -79.9930),        ['"duquesne university"']),
    ('Lehigh University',                       (40.6071, -75.3790),        ['"lehigh university"']),
    ('Pennsylvania State University',           (40.7961, -77.8627),        ['"pennsylvania state university"', '"penn state university"']),
    ('Temple University',                       (39.9800, -75.1600),        ['"temple university"']),
    ('University of Pennsylvania',              (39.9500, -75.1900),        ['"university of pennsylvania"']),
    ('University of Pittsburgh',                (40.4445, -79.9532),        ['"university of pittsburgh"']),

    # Rhode Island
    ('Brown University',                        (41.8262, -71.4032),        ['"brown university"']),
    ('University of Rhode Island',              (41.4807, -71.5258),        ['"university of rhode island"']),

    # South Carolina
    ('Clemson University',                      (34.6783, -82.8391),        ['"clemson university"']),
    ('University of South Carolina',            (33.9975, -81.0252),        ['"university of south carolina"']),

    # South Dakota
    ('South Dakota State University',           'south dakota state university',            ['"south dakota state university"']),
    ('University of South Dakota',              'university of south dakota',               ['"university of south dakota"']),

    # Tennessee
    ('University of Memphis',                   (35.1190, -89.9370 ),       ['"university of memphis"']),
    ('University of Tennessee',                 (35.9516, -83.9300),        ['"university of tennessee"']),
    ('Vanderbilt University',                   (36.1486, -86.8049),        ['"vanderbilt university"']),

    # Texas
    ('Baylor University',                       'baylor university',                        ['"baylor university"']),
    ('Rice University',                         'rice university',                          ['"rice university"']),
    ('Southern Methodist University',           'southern methodist university',            ['"southern methodist university"']),
    ('Texas A&M University',                    'texas a&m university',                     ['"texas a&m"', '"texas a & m"']),
    ('Texas Tech University',                   'texas tech university',                    ['"texas tech university"']),
    ('University of Houston',                   'university of houston',                    ['"university of houston"']),
    ('University of North Texas',               'university of north texas',                ['"university of north texas"']),

    ('University of Texas, Arlington',          'university of texas arlington',            ['"university of texas", arlington']),
    ('University of Texas, Austin',             '[university]University of Texas, Austin',  ['"university of texas" austin']),
    ('University of Texas, Dallas',             'university of texas dallas',               ['"university of texas", dallas']),
    ('University of Texas, El Paso',            'university of texas el paso',              ['"university of texas", el paso']),
    ('University of Texas, San Antonio',        'university of texas san antonio',          ['"university of texas", san antonio']),

    # Utah
    ('Brigham Young University',                'brigham young university',                 ['"brigham young university"']),
    ('University of Utah',                      'university of utah',                       ['"university of utah"']),
    ('Utah State University',                   'utah state university, logan',             ['"utah state university"']),

    # Vermont
    ('University of Vermont',                   (44.4760, -73.1950),        ['"university of vermont"']),

    # Virginia
    ('College of William & Mary',               (37.2710, -76.7074),        ['"college of william" mary']),
    ('George Mason University',                 (38.8308, -77.3075),        ['"george mason university"']),
    ('Old Dominion University',                 (36.8865, -76.3052),        ['"old dominion university"']),
    ('University of Virginia',                  (38.035 , -78.5050),        ['"university of virginia"']),
    ('Virginia Commonwealth University',        (37.5466, -77.4532),        ['"virginia commonwealth university"']),
    ('Virginia Tech',                           (37.2250, -80.4250),        ['"virginia tech"', '"virginia polytechnic"']),

    # Washington
    ('University of Washington',                'University of Washington',                 ['"university of washington"']),
    ('Washington State University',             'washington state university',              ['"washington state university"']),

    # Washington D.C.
    ('Catholic University of America',          (38.9329, -76.9977),        ['"catholic university of america"']),
    ('Georgetown University',                   (38.9072, -77.0727),        ['"georgetown university"']),
    ('George Washington University',            (38.9007, -77.0508),        ['"george washington university"']),
    ('Howard University',                       (38.9222, -77.0194),        ['"howard university"']),

    # West Virginia
    ('West Virginia University',                (39.6358, -79.9545),        ['"west virginia university"']),

    # Wisconsin
    ('University of Wisconsin, Madison',         (43.075,  -89.4172),       ['"university of wisconsin" madison']),
    ('University of Wisconsin, Milwaukee',       (43.0750, -87.8829),       ['"university of wisconsin" milwaukee']),

    # Wyoming
    ('University of Wyoming',                   'university of wyoming, laramie',           ['"university of wyoming"'])

]

def parse_locations(source_db_path, geoloc_db_path):

    successful_parses = set()
    failed_parses = set()

    source_connection = sqlite3.connect(source_db_path)
    source_cur = source_connection.cursor()

    geoloc_connection = sqlite3.connect(geoloc_db_path)
    geoloc_cur = geoloc_connection.cursor()

    count = 0

    sql_query = "SELECT distinct(affiliation) from ref_authors order by rowid desc limit 1000"

    count = 0
    for row in source_cur.execute(sql_query):
        count += 1
        if count % 10000 == 0:
            print count

        raw_location = row[0]
        if raw_location == '' or raw_location is None:
            continue

    #         cursor.execute("SELECT rowid FROM components WHERE name = ?", (name,))
    # data=cursor.fetchall()
    # if len(data)==0:
    #     print('There is no component named %s'%name)
    # else:
    #     print('Component %s found with rowids %s'%(name,','.join(map(str,zip(*data)[0]))


        geoloc_cur.execute(u'SELECT * FROM location_to_id where raw_name = "{}";'.format(raw_location))
        results = geoloc_cur.fetchall()
        if len(results) > 0:
            continue


        parse_and_add_location(raw_location, geoloc_connection)




    print "Successful: {}. Failed: {}".format(len(successful_parses), len(failed_parses))



def create_geoloc_db(db_path):

    connection = sqlite3.connect(db_path)
    cur = connection.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS locations(
                    id      INTEGER PRIMARY KEY,
                    name    TEXT UNIQUE,
                    lon     REAL,
                    lat     REAL
                    );''')


def add_fulltext_search_to_affiliation(source_db_path):

    db = sqlite3.connect(source_db_path)
    cur = db.cursor()

    cur.execute('create virtual table affiliation_lookup using fts4(id, affiliation);')
    cur.execute('insert into affiliation_lookup select id, affiliation from ref_authors;')
    db.commit()

def word_counter(source_db_path):

    source_connection = sqlite3.connect(source_db_path)
    source_cur = source_connection.cursor()

    word_count = Counter()

    count = 0

    sql_query = "SELECT distinct(affiliation) from ref_authors order by rowid desc limit 1000000;"
    ngram = 3

    for row in source_cur.execute(sql_query):

        if row[0] is None:
            continue
        text =  row[0].lower()
        text = re.findall(r'[\w]+', text)
        for i in range(len(text) -3 +1):
            word_count[' '.join(text[i:i+3])] += 1

    for row in word_count.most_common(10000):
        if row[0].find('university') > -1:
            print row

def university_counter(source_db_path):

    source_connection = sqlite3.connect(source_db_path)
    source_cur = source_connection.cursor()

    total_count = 0

    for university in UNIVERSITIES:

        local_count = 0
        for query_name in university[2]:

            source_cur.execute(u'''SELECT count(*) from affiliation_lookup where affiliation match '{}'; '''.format(query_name))
            count = source_cur.fetchall()[0][0]
            local_count += count
            total_count += count

        print "{}: {}".format(university[0], local_count)

    print "Total count: {}".format(total_count)

if __name__ == "__main__":

    geoloc_db_path = '/tobacco/geoloc.db'
    source_db_path = "/tobacco/medline_complete_normalized.db"

#    word_counter(source_db_path)
    university_counter(source_db_path)

#    add_fulltext_search_to_affiliation(source_db_path)

#    create_geoloc_db(geoloc_db_path)
#    parse_locations(source_db_path, geoloc_db_path)