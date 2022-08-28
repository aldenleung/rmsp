from setuptools import setup, find_packages

with open("README.md", "r") as readme_file:
	readme = readme_file.read()

requirements = ["python-dateutil", "dill", "networkx"]

setup(
	name="rmsp",
	version="0.0.1",
	author="Alden Leung",
	author_email="alden.leung@gmail.com",
	description="Resource management system for python",
	long_description=readme,
	long_description_content_type="text/markdown",
	url="https://github.com/aldenleung/rmsp/",
	packages=find_packages(),
	install_requires=requirements,
	classifiers=[
		"Programming Language :: Python :: 3.7",
		"Programming Language :: Python :: 3.8",
		"Programming Language :: Python :: 3.9",
		"License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
	],
)

