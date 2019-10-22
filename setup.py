import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="avro_validator",
    version="1.0.0",
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
    keywords="avro schema",
    project_urls={
        "Repository": "https://github.com/leocalm/avro_validator",
        "Bug Reports": "https://github.com/leocalm/avro_validator/issues",
        # "Documentation": "https://arrow.readthedocs.io",
    }
)
