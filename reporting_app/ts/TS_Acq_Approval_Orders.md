# TS_Acq_Weekly_Approval_Orders
This report is run weekly on Fridays. It returns purchase order line information about all approval orders created from the previous Friday to the Thursday before the report is run.

Report is set to have a default start date of current_date - 7 and an end date of current_date.

The report returns the following fields: vendor, fund, account number, receiving note, internal note, requester, estimated price, PO line number, title, contributors, product ids (ISBN/OCLC numbers/etc), publisher, edition, and invoice number associated with the purchase order.
