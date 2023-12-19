# Airflow Dag

It's a simple etl process which migration data from database (PostgreSQL) to another database (PostgreSQL) create by Data Vault Architecture.
A few tasks run parallel. Tasks use cross-communications (Xcom). Pandas provide comminicate to database. Build Airflow in docker-compose file.

# First Step - Design Database by Data Vault

You need thinking how will display database in data vault architecture. You need make Hubs, Satellites, Links. 

- My Hubs: customer, product, order. 
- My Satellites: customer, product, order. 
- My link: order_customer, order_product. 

### Original Database

<img src="https://github.com/OnvoKoder/MigrationAirflowDag/assets/65452318/94940231-a7a2-43e0-b886-028ca20314d1" width="1000" height="500"/>

### Data Vault

<img src="https://github.com/OnvoKoder/MigrationAirflowDag/assets/65452318/a042c92b-272f-4b93-974d-831fff0e33a3" width="1000" height="500"/>

# Second Step - Create procedure for Data Vault

My procedure:
- insert_customer
- insert_product
- insert_order
- insert_order_product
- insert_order_customer

# Third Step Design Dag

<img src="https://github.com/OnvoKoder/MigrationAirflowDag/assets/65452318/47188458-f6dd-4219-948b-116b13d7945e" width="1000" height="500"/>
