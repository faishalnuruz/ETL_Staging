#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 16 11:05:33 2018

@author: faishal
"""

import sys
import logging
import pandas as pd
import petl as etl
from sqlalchemy import create_engine
from collections import OrderedDict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    #DB_Source
    conn = create_engine('postgresql://odoo:rc22ODWbBNnm79QZvZ@warpindb-prod-ap-southeast-1b.caplyqfnwfh3.ap-southeast-1.rds.amazonaws.com:5432/odoo')
    #DB_Target
    conn_target = create_engine('postgresql://wpsdatawh:1ribuwarungpintar@warpindatawh.caplyqfnwfh3.ap-southeast-1.rds.amazonaws.com:5432/warpindatawh')
except:
    logger.error("ERROR: Unexpected error: Could not connect to PostgreSQL instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS PostgreSQL instance succeeded")

cur = conn.cursor()
cur_target = conn_target.cursor()

#Source
table = etl.fromdb(conn, """select 
                 res_company.name, 
                 sum(product_qty) as qty, 
                 sum(price_total) as total 
                 from report_pos_order 
                 inner join res_company on res_company.id = report_pos_order.company_id 
                 where date(report_pos_order.date AT TIME ZONE 'GMT +7') = current_date 
                 group by res_company.name
                 order by sum(price_total) desc""")

#Transformation
#grouping with aggregation
aggregation = OrderedDict()
aggregation['qty'] = 'qty', sum
aggregation['total'] = 'total', sum
table5 = etl.aggregate(table, 'name', aggregation)
dfsum = etl.todataframe(table5)

#Target
dfsum.to_sql('GMV Warung', conn_target, if_exists='replace', index=None)