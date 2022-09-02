# STORE INVENTORY BATCH PROCESSING PIPELINE

### GOAL
To create a centralized database , that contains stock details of all the warehouses of company in every country or cities.
Each warehouse has its own database.Centralized database should update on daily basis at day end.

#### Components:

* DailyStockUploaderJob:
  Responsible to upload current inventory status of warehouse at day end
  This would be a schedule job , that can execute on each local warehouse.


