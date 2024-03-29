from setuptools import setup, find_packages

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()


__version__ = "0.4.1"
REPO_NAME = "datamigrato"
PKG_NAME= "datamigrato"
AUTHOR_USER_NAME = "ritikdutta"
AUTHOR_EMAIL = "ritikduttagd@gmail.com"


setup(
    name=PKG_NAME,
    version=__version__,  # Starting with an initial version. Update as necessary.
    author=AUTHOR_USER_NAME,
    author_email=AUTHOR_EMAIL,
    description='A Python package for migrating data between various databases.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url=f'https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}',
    project_urls={
        "Bug Tracker": f"https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}/issues",
    },
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
)
