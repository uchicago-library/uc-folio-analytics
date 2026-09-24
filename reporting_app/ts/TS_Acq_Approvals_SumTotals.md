# TS_Acq_Approvals_SumTotals

## Purpose
This report returns the sum of purchase order line estimated prices and invoice line totals to see spending by vendor and fund for JRL and Law from the beginning of the fiscal year through the day before the report is being run.

## Parameters
The report uses a start date of July 1st for the fiscal year. This needs to be updated in the SQL function at the start of each fiscal year but does not need to be entered in the UI.

The end_date parameter is the date the report is being run. The report includes approval orders created on dates up to but not including this date.

## Output
This report returns bill to name, vendor code, pol fund codes, requester (sub funds), pol account number, pol estimated cost totals, invoice fund codes, and invoice line totals which are grouped by vendor, bill to, pol fund codes, and invoice fund codes.

## Notes for future development
The report currently uses some derived tables and should be updated to use all live data.