[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare SAF Connector

## 🔗  Quick Links
- [Docs](https://tuva-health.github.io/the_tuva_project/#!/overview): Learn about the Tuva Project data model
- [Knowledge Base](https://thetuvaproject.com/docs/intro): Learn about claims data fundamentals and how to do claims data analytics
<br/><br/>

## 🧰 What does this project do?

This connector is a dbt project that runs the Tuva Project on the Medicare SAF claims data, including mapping raw Medicare SAF claims to the Tuva Claims Data Model (CDM), loading all terminology sets, and running all data marts.

You can read more about Medicare SAF claims data [here](https://www.cms.gov/Research-Statistics-Data-and-Systems/Files-for-Order/LimitedDataSets/StandardAnalyticalFiles).
<br/><br/>

## 🔌 Database Support

- BigQuery
- Redshift
- Snowflake
<br/><br/>

## ✅ Quick Start Instructions

1. Fork or clone this repo to your local machine or environment.

2. Update the dbt_project.yml file to connect to your data warehouse and point to the database, schema, and tables where your Medicare SAF data is stored.

3. Execute `dbt build` from the command line to execute the entire project.
<br/><br/>

## Medicare SAF Data Issues
In this section we catalog important data quality issues present in the Medicare SAF data and how we've accounted for those issues in this connector.  This isn't an exhaustive inventory of every data issue.  Rather, we focus on notable issues that impact downstream analytics in the Tuva Project.

- **Claim From Date:** The Medicare SAF data only includes Claim Thru Date, so we don't have a claim date of service start date.  For inpatient claims we've elected to map admission date to the claim_start_date in the Tuva Claims Data Model.


<br/><br/>

## 🙋🏻‍♀️ How is this project maintained and can I contribute?

### Project Maintenance

The Tuva Project team maintaining this project **only** maintains the latest version of the project. We highly recommend you stay consistent with the latest version.

### Contributions

Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached on our Slack community.
<br/><br/>

## 🤝 Community

Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
