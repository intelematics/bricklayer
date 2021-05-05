import setuptools

with open('bricklayer/__version__.py') as fd:
    version = fd.read().split('=')[1].strip().strip("'")

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bricklayer",
    version=version,
    author="Intelematics",
    description="Internal Databricks utils",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/intelematics/bricklayer",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'databricks_cli', 'shapely', 'folium'
    ]
)
