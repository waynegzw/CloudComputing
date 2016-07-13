# YouKnowNothingJonSnow
### Phase2 live test


#####Data Source:
- CMU: `s3://cmucc-datasets/twitter/s16/part-00000` 
- q2 raw: s3://
- q2 data: 9 partitions: s3://etlprj35/slice_9_X_****_***** (x is the xth partition out of 9, *** is range)  
use s3 ls command to list
file names:  
s3://etlprj35/slice9_1_0_153938223  
s3://etlprj35/slice9_2_153938224_336171212  
s3://etlprj35/slice9_3_336171213_547447402  
s3://etlprj35/slice9_4_547447402_955816538  
s3://etlprj35/slice9_5_955816538_1481405995  
s3://etlprj35/slice9_6_1481405995_2171466339  
s3://etlprj35/slice9_7_2171466339_2337873626  
s3://etlprj35/slice9_8_2337873626_2420383813  
s3://etlprj35/slice9_9_else
- q3 data:  
s3://etlprj35q3/q3slice_5_1_0_186242860  
s3://etlprj35q3/q3slice_5_2_186242840_383026777  
s3://etlprj35q3/q3slice_5_3_383026777_637142417  
s3://etlprj35q3/q3slice_5_4_637142424_1467051504  
s3://etlprj35q3/q3slice_5_5_1467051524_2366272589  
s3://etlprj35q3/q3slice_5_6_2366272589_else  
 

#####Cloudera installation
- http://www.cloudera.com/documentation/enterprise/latest/topics/cm_ig_install_path_a.html  
`` wget https://archive.cloudera.com/cm5/installer/latest/cloudera-manager-installer.bin `` 
`` chmod u+x cloudera-manager-installer.bin  ``
`` sudo ./cloudera-manager-installer.bin``
- Admin site: dns:7180 admin admin
- 
