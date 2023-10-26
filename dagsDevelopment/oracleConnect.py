
import cx_Oracle 
connection = cx_Oracle.connect("IBLGRPHCM/iblgrp106hcm@Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com/6464")
print (connection.version)
connection.close()