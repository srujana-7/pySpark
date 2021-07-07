from pyspark import SparkContext
sc = SparkContext.getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from pyspark.sql.functions import regexp_replace, trim, col, lower
#Function to clean an input text file with punctuations and convert to lowercase 
def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return trim(lower(regexp_replace(column, '([^\s\w_]|_)+', ''))).alias('sentence')



fileName="s3://python-spark-project/Word_Count_Data.txt"


DF = sqlContext.read.text(fileName).select(removePunctuation(col('value')))
DF.show(15, truncate=False)
DF.count()

from pyspark.sql.functions import split, explode

DF2 = (DF .select(split(DF.sentence, '\s+').alias('split')))
DF3 = (DF2.select(explode(DF2.split).alias('word')))

DF3.show()

wordListDF= DF3.groupBy('word').count()

wordListDF.show(100)
wordListDF.count()
