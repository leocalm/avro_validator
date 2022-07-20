import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="avro_validator",
    version="1.1.1",
    author="Leonardo de Campos Almeida",
    author_email="leocalm@gmail.com",
    description="Pure python avro schema validator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/leocalm/avro_validator",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[],
    extras_require={
        "dev": [
            "pytest",
            "coverage",
            "tox",
            "flake8",
            "hypothesis",
            "pytest-cov",
            "python-coveralls",
        ],
        "docs": [
            "sphinx",
            "pallets-sphinx-themes",
            "sphinxcontrib-log-cabinet",
            "sphinx-issues",
        ],
    },
    keywords="avro schema",
    project_urls={
        "Repository": "https://github.com/leocalm/avro_validator",
        "Bug Reports": "https://github.com/leocalm/avro_validator/issues",
        "Documentation": "https://avro-validator.readthedocs.io/",
    },
    entry_points={
        "console_scripts": ["avro_validator=avro_validator.cli:main"]
    },
)
