from setuptools import setup
#from distutils.core import setup
from Cython.Build import cythonize
import numpy as np

def run():
    setup(name='medline',
          version = '0.04',

          packages=['medline',
                    'medline.database',
                    'medline.query',
                    'medline.topic_modeling',
                    'medline.utilities',
                    'medline.web_dev'],
          include_package_data=True,
#          package_data= {'': ['data.*', 'utilities.*'],},
          ext_modules=cythonize(['medline/query/*.pyx'],
                                compiler_directives={"embedsignature": True}),
          include_dirs=[np.get_include()],
          zip_safe=False,
          )



if __name__ =="__main__":
    run()