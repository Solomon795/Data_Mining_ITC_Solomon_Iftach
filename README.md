# Program goal
The aim of the program is to extract data from the ResearchGate site (https://www.researchgate.net/) about articles in the energy-market.
The chosen pages provide information about the title, abstract, authors, citations, publication date and journals.
Based on this data we wish to gain a perspective on the energy-market, its status and the new directions that are being explored.

## Installation
The program has been developed in Pycharm IDE, using Selenium web driver and Beautifulsoup
(as described in the requirements).

Note, if errors about lxml appear during the execution of the program then install the 'lxml' package in the pycharm interpeter following these steps:

The program uses a configuration file named config.info, that should reside in the running directory of the program. 

## Usage 
All parameters needed for the program, are taken from the JSON configuration file:
 - User(email) and password for the login to the ResearchGate site  
 - Topic name 
 - The number of the pages that should be processed for the topic. 
 Note: each page contains 10 references.
