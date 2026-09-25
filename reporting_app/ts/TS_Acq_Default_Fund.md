# TS_Acq_Daily_Default_Fund
This report is run daily and looks for purchase orders created in the last seven days with a fund of GCOMG.

This report does not require parameter entry because it looks for dates within seven days of the current date.

The report returns the following fields: vendor, fund, account number, estimated price, po line number, title, publisher, edition, internal note, date created, created by user name, and workflow status.

Filters used: fund code = GCOMG, user name is restricted to staff who do acquisitions data loading, and date > current_date - INTERVAL '7 days'.

Consider future updates to change the interval length or to end the report on an earlier date for different date ranges.