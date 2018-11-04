# Title : World through media

Team members : Alice Bizeul, Johan Cattin, Laure Font


# Abstract

The starting point of this project was to ask ourselves : how are we connected to the world ? 
The answer seems obvious. Except in specific occasions where travelling is an option, media is our main window on the rest of the globe, especially regarding foreign countries where language and distance generate a gap between us and the information. This highlights our vulnerability to any sort of bias media could convey. 
Another starting point is the actual mood trend which consists of questionning the reliability of medias and their integrity. A few examples are mentionned here below.
Using the GDELT 2.0 database, we wish to give insights on the image of the world conveyed by medias and highlight possible bias. This dataset focuses on conflictual events related by media in the world since 2015. As a complementary source of information we will also be using the Uppsala Conflict Data Program which traces information about armed conflict worldwide. 
Raising awareness with datascience on this topic can sharpen, if not society's, our look when grasping information through media.


# Research questions

Here are some research questions we would like to address during the project : 

    - What are the distributions of human activity and media coverage worldwide ?
    - Can media coverage be an indicator of a country's stability or state of activity ? If not, can we extract different parameters that show media coverage bias ?
    - Do the features of an event give an indication of the level of mediatic attention it will receive ? (type of event, number of deaths in armed conflicts, level of internalization, geographic position) 


# Dataset

For this project we will be using two databases : 

1. The global database of society 2.0 (GDELT) is a database which monitors the information provided by broadcast, prints and web news worldwide. It grasps the information of human activity, processes it to provide different indicators and upload the database which is therefore updated every 15 minutes. As its size and complexity are huge, data is available on Google's Big Query using standard SQL. 
The information provided by the database are Event IDs, Date of event, Actors (Code, Name, Country Code, Religion Code, Ethnicity Code, Type of Actor), Mention if the event is a root event or not, Goldstein Scale, Number of Mentions in the News, Number of sources, Average Tone of the mentions, Geographic Information (Country Code, Latitude Longitude etr..) as mentioned in the CookBook (http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)

A second dataset is provided in the GDELT 2.0 program called a Mention Tables, it processes all new mentions of an event in media, worldwide and provides several information of all new mention. It makes it possible to track an event through media over time and get indications on the type of mention.

Events, Goldstein Scale values are among the data types that are coded under the form of CAMEO index, their corresponding values are mentionned in the tables above: 
- https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt
- https://www.gdeltproject.org/data/lookups/CAMEO.goldsteinscale.txt

2. The Uppsala Conflict Data Program (UPCD) is a database which provides data on organized violence since 1949. The UCDP/PRIO Armed Conflict Dataset version 18.1 provides the general information on armed conflicts whereas the UCDP Battle-Related Deaths Dataset provides the estimate of the number of deaths for each armed conflict. The information of interest to use is the location of the conflict, its date as well as the number of deaths associated. All of these information are provided by these databases.

As the GDELT only contains information as from February 2015, the UPCD database will be filtered in order to keep only relevant information.

These information were provided by the corresponding cookbooks : 
- http://ucdp.uu.se/downloads/ucdpprio/ucdp-prio-acd-181.pdf
- http://ucdp.uu.se/downloads/ucdpprio/ucdp-prio-acd-181.pdf


# A list of internal milestones up until project milestone 2

1. Acquisition of the data, extensive exploration, understanding the structure 
2. Update on the questions we would like to answer
3. Extraction of the data of interest and data cleaning
4. Statistical analysis of the data 
5. Find methods to determine correlation between the different parameters we are interested in


# Questions for TAs



