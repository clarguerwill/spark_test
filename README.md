## Python Packages to Import
* pandas
* xmltodict



## Output Parquet Schema
```
root                                                                            
 |-- source: string (nullable = true)
 |-- sourceId: string (nullable = true)
 |-- sidType: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- middleNameList: array (nullable = false)
 |    |-- element: string (containsNull = true)
 |-- titleList: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- positionList: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- dobMap: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: string (valueContainsNull = true)
 |-- aliasStruct: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- aliasType: string (nullable = true)
 |    |    |-- aliasQuality: string (nullable = true)
 |    |    |-- firstName: string (nullable = true)
 |    |    |-- lastName: string (nullable = true)
 |    |    |-- middleNameList: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 ```


