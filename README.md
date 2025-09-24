# Scanario-Question
Solving with .withColumn , when and Group by collect
Group the Age and get me the count of below Data.  
#  Input Data:  
![Input data](https://github.com/user-attachments/assets/65b327e6-d5ab-48bd-ab2e-993ad53c7624)  

# Solution:.  
data = [(1,'Alice',25,'F'),(2,'Bob',40,'M'),(3,'Raj',46,'M'),(4,'Sekar',66,'M'),(5,'Jhon',47,'M'),(6,'Timoty',28,'M'),(7,'Brad',90,'M'),(8,'Rita',34,'F')]  
columns = ["customer_id","name","age","gender"]  
df = spark.createDataFrame(data,columns)  
df.show()  


groupdf = df.withColumn(  
    "age_group",  
    when((col("age") >= 19) & (col("age") <= 35), "19-35")  
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")  
    .when(col("age") > 51, "51+")  
    .otherwise("Other")  
)  

groupdf.show()  


finaldf = groupdf.groupBy("age_group").agg(  
               count("age_group").alias("count")  
)  

finaldf.show()  
