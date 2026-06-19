# Athena-Lamda-Splunk-Integration
The main.py file gives you a template on how to read big data sets stored in S3 like VPC Flow logs/ Cloudtrail/ EDR telemetry/ ELB access logs etc. When we donot have enough license to ingest everything into Splunk SIEM, we can leverage this solution to get critical insights into splunk for detections/threat hunt/threat intel correlation. This gives a jump start on Incident reponse and data correlation.

Most of the code has been taken in a modular fashion using ChatGPT and put together for the a working solution. Feel free to customize as per your source data and change the Splunk Ingest method to suit your environment.

References:

https://docs.aws.amazon.com/athena/latest/ug/vpc-flow-logs.html

https://docs.aws.amazon.com/athena/latest/ug/cloudtrail-logs.html

https://docs.aws.amazon.com/athena/latest/ug/elasticloadbalancer-classic-logs.html

https://docs.aws.amazon.com/athena/latest/ug/application-load-balancer-logs.html

https://docs.aws.amazon.com/athena/latest/ug/waf-logs.html

https://www.youtube.com/watch?v=a_Og1t3ULOI

https://medium.com/@shivakumar.mcet/aws-fetch-data-from-amazon-athena-using-api-gateway-and-aws-lambda-4e5729519940

