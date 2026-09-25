# TS_Acq_Zero_EstPrice
This report is run daily and looks for purchase orders created in the last seven days where the estimated price is zero. It excludes replacement copies (orders with a receiving note with the text 'replacement').

This report does not require parameter entry because it looks for dates within seven days of the current date.

The report returns the following fields: vendor, fund, account number, estimated price, po line number, title, ISBNs, publisher, edition, date created, created by user name, workflow status, and acquisitions method.

Filters used: POL estimated price = 0, acquistions method is Purchase at Vendor system or Approval plan, user name is restricted to staff who do acquisitions data loading, workflow status is open, and date > current_date - INTERVAL '7 days'.

Consider future updates to change the interval length or to end the report on an earlier date for different date ranges.
