# US Accidents

An interviewer asked me 'What kind of exposure to SQL have you had?'
and I felt that my answer of 'one uni subject and some hacker-rank' 
practice problems' was insufficient. This project will demonstrate
- Database administration (creating a database, creating users with
appropriate permissions, integrity constraints); 
- ETL pipeline for larger than memory datasets;
- Geospatial analysis and visualisation.

Tools used include *PostgreSQL*, *Python (Polars)*, *Apache Sedona*.




## References
- Moosavi, Sobhan, Mohammad Hossein Samavatian, Srinivasan Parthasarathy, and Rajiv Ramnath. “A Countrywide Traffic Accident Dataset.”, arXiv preprint arXiv:1906.05409 (2019).
- Moosavi, Sobhan, Mohammad Hossein Samavatian, Srinivasan Parthasarathy, Radu Teodorescu, and Rajiv Ramnath. “Accident Risk Prediction based on Heterogeneous Sparse Data: New Dataset and Insights.” In proceedings of the 27th ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems, ACM, 2019.




## Steps:
- [ ] Create a database with an admin user for creating/modifying
tables and normal user who can only query.
    - [x] Create a database.
    - [x] Create an admin user.
    - [x] Create a normal user.
    - [x] Add the postgis extension to the database.
- [ ] Build ETL pipeline for the US-accidents data.
    - [x] Parse dates/times. `polars.scan_csv(..., try_parse_dates=True)` does it.
    - [ ] Cyclical encoding of hour of the day.
- [ ] Source data on by-county population data and develop ETL.
- [ ] Same for state/county boundary shapefiles.
- [ ] Maybe: Same for geographical data.
- [ ] Run all ETL steps with `systemd-run` and a limit on memory
to demonstrate workflow for larger than memory data.
- [ ] Data Integrity: after the various data sources have been
brought in, they need to be subjected to data integrity checks, 
e.g. every state code that appears in the 'accidents' table must 
correspond to a an entry in the 'states' table.
- [ ] Explore this database in apache sedona.

## Some Ideas for Plots
- Plot all cities called Washington
- Plot cities names after Lincoln or other aboloitionists vs cities
named after known confederates (not that this has anything to do 
with traffic accidents!)
- Plot cities named after European capitals; 
- Plot mean severity for each county (for meaningful cross county
comparison) or each pixel (for smoother gradient);
- Compare a normal day with the worst days of the year and maybe
animate a 24 hour period. 

