# Movies-ETL

"challenge.py" defines the movie_function() function, which should run all desired steps of ETL.

There are several assumptions in the code. For one, "challenge.py" also calls the function using my file paths. The arguments would likely have to change if another computer were to try the call I included.

Secondly, the criteria for the "wiki_columns_to_keep" variable, defined on line 89, could change. The criteria keeps columns with at least 10% non-null values. The columns included and excluded could therefore change after updating the data with more movies (depending on their null values).

Thirdly, the code assumes certain columns exist, such as 'Box office'. If those columns don't exist, or have different names on some wikipedia pages, data will be lost.

Fourth, despite some robust use of regular expressions, the code still makes some assumptions about the data types/formats within specific columns. Values that don't get captured using the defined regex could be lost.

Fifth, the path to the PostgreSQL database works in part because I've included my database's password and IP address. Another user will likely have a different password, and will need to change "config.py" as a result.
