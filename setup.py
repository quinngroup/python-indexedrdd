try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'Python port of IndexedRDD',
    'author': 'Narita Pandhe',
    'url': 'https://github.com/quinngroup/python-indexedrdd.git',
    'download_url': 'https://github.com/quinngroup/python-indexedrdd.git',
    'author_email': 'narita.pandhe@gmail.com',
    'version': '0.1',
    'install_requires': ['pytest'],
    'packages': ['indexedrdd'],
    'scripts': [],
    'name': 'python-indexed-rdd'
}

setup(**config)
