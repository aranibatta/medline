'''
Configurations for mysql servers, both local and on aws
Feel free to add your configs if you use the medline db on your local machines
'''


DB_CONFIGS = {

    'stephan_local': {
            'hostname': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'aoeusnth',
            'database': 'medline'
    },

    'aws': {
            'hostname': 'xyz',
            'port': 3306,
            'user': 'statnews',
            'password': 'aoeusnth',
            'database': 'medline'
    }
}