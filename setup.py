from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="migres",
    version="0.1.0",
    author="Mayowa Ogungbola",
    author_email="ogungbolamayowa@email.com",
    description="Database migration tool for MariaDB to PostgreSQL/Supabase",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Phenzic/migres",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pymysql>=1.0.0",
        "psycopg2-binary>=2.9.0",
        "pandas>=1.3.0",
        "configparser>=5.0.0",
    ],
    entry_points={
        "console_scripts": [
            "migres=migres.cli:main",
        ],
    },
)