Trying out what is involved in getting DynamoDB to support a conditional upsert.

 - If a record with the specified id does not already exist, then insert
 - If a record with the specified id already exists
   - If the specified version is greater than the existing version then overwrite the record with the newer, later version.
   - If the specified version is less than or the same as the existing version then leave the existing record as it is.