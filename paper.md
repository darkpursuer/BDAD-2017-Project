A study on the relationship between housing prices and the air quality, metro stations and crimes in NYC
=======

- Yi Zhang
Department of Computer Science
New York University, United States
yz3940@nyu.edu

- Taikun Guo
Department of Computer Science
New York University, United States
tg1539@nyu.edu

## Abstract
How the housing prices are affected by their surrounding environments? Though it's a tough work to figure out all of the related factors. In this study, we mainly focus on the factors of the air quality, metro stations and crimes, and exploit how they influenced the housing prices in New York City (NYC). To do this, we profiled and compiled historical information on changes in housing prices, air quality, metro stations and crimes from 2010 till now in all the boroughs and blocks in NYC. With the refined data, we calculated the scores of each factors in each blocks with different measurements, then trained and tested some distinct multiple linear regression models which implied a large and statistically significant association between housing prices and these three factors. (And the result will tell us which factor plays the most important role on the housing prices...)

## Keywords
- Housing prices
- Air quality
- Metro
- Crime
- Analytics
- Big Data

## Introduction
It's an intersting task to discover the law how the housing prices increased till now. However, it's also a complicated and tough work to figure out all the factors that affect the housing prices. For example, the location of your house, the nearby metro lines and your working place determine how long will you take to reach your destinations, the environment quality and noise environment affect the degree of comfort of your house, and the criminal situations and the locations of nearby police department and fire department influence you sense of security. And, definitely, the most important thing to be considered is the price of your house or apartment. 

In this study, we chose three main factors, air quality, metro stations and crime, to simulate their relationships with the housing prices, which implied there is a statistically significant relationship between housing and these factors. And we built some multiple linear regression models (To consider the influence of the imbalance of development, we built one model for each borough in NYC. As mentioned by Glaeser (2005) <sup>[4]</sup> , the cost of land and construction played an important role in the housing prices) to approach a dwelling prices function which can estimate the housing price. And with the predicted housing prices and the historical housing prices, we can make some analytics on the prices of houses in NYC and give some recommendations for consumers.

First, we will introduce the motivation of our study and describe why we think this application is important. Then, some related work will be introduced and compared with our work, which underscores the creativity of our study. In the fourth part, the paper will show how we design the application, including how the application was built and how the data flew. Then in the experiments part, we will describe our experimental setup and discuss our experiments results. Then we will reach our conclusion in the next section.  


## Motivation
Housing prices has been one of the most important things that need to be considered by people in the United States, especially in those megacities like New York City, the Bay Area, etc. As Glaeser (2005) <sup>[1]</sup> mentioned, the housing price has been kept increasing in the past 60 years. And this is always a heavy burden for those new young graduates or poors. Besides the prices of houses, it's also important to consider the housing environments, such as air quality, metro stations, crime, etc. Thus, to help consumers choosing a favored house with lower price and good environments is urgent. The aim of our study it to build a Big Data application in order to help consumers make wise decisions on where and how to buy their desirable houses.

## Related Work
We barely found a paper that considered multiple factors affecting the housing prices, and most of the papers only focused on one factor, such as air quality, crime, subway line or locations. For the factor of air quality, Vincenza (2014) <sup>[2]</sup> provided the relationship between housing prices and air quality by building some hedonic multiple linear regression models to estimate the data in the province of Taranto, and his model also considered the house types, metro lines, roads and the sources and types of pollution. Though this is good experiments, his data source and size were not so perfect. His data was from the survey of more than 1000 observations, and such little data cannot ensure the precision and universality of the experiment data.

For the aspect of crime, Devin (2012) <sup>[3]</sup> gave a good experiment on the association between crime and property values by focusing on the 1990s crime drop with enormous and persuasive data, and reached a conclusion that the reduction in crime surely has an immediate benefit that should be reflected in housing values. However, the 1990s crime drop was irreproducible and peculiar, so that it cannot be considered as a normal simulation for our study. So in our study, we should more focus on the relationship between the housing prices and crime under normal circumstances.

In addition, Agostini (2008) <sup>[5]</sup> presented his conclusion that building new metro lines will prompt the surrounding housing prices to rise in his paper. He also considered various factors inluding the distance to clinic, school and metro stations, which is a good and useful estimation for us to estimate the scores of distance of houses to nearest metro stations in our study.

## Design

![alt text](https://github.com/billg1990/nyu-bigdata-application-project/blob/master/Data%20Flow.jpg)

## References
[1] Glaeser, Edward L., Joseph Gyourko, and Raven Saks. Why have housing prices gone up?. No. w11129. National Bureau of Economic Research, 2005.

[2] Chiarazzo, Vincenza, et al. "The effects of environmental quality on residential choice location." Procedia-Social and Behavioral Sciences 162 (2014): 178-187.

[3] Pope, Devin G., and Jaren C. Pope. "Crime and property values: Evidence from the 1990s crime drop." Regional Science and Urban Economics 42.1 (2012): 177-188.

[4] Glaeser, Edward L., Joseph Gyourko, and Raven Saks. "Why is Manhattan so expensive? Regulation and the rise in housing prices." The Journal of Law and Economics 48.2 (2005): 331-369.

[5] Agostini, Claudio A., and Gastón A. Palmucci. "The anticipated capitalisation effect of a new metro line on housing prices." Fiscal studies 29.2 (2008): 233-256.
