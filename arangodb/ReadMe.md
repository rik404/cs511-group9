# ArangoDB

Please install docker before proceeding further.

## Docker Settings

Please ensure that the docker is configured to have a minimum of
- Swap: 4GB
- Virtual Disk Limit: 128GB

## Dataset Generation
Please refer to the ```koalabench``` folder on how to generate the dataset. Please generate datasets of flat data model and place them inside the folder ```data```

## Running the experiments

Please clear the existing images and containers manually or use the script

```bash
bash cleanup.sh
``` 
To spin up all the containers use the command

```bash
bash start-all.sh
```

Once the containers are deployed, enter the main container shell using
```bash
docker-compose -f cs511p1-compose.yaml exec main bash
```
Load the data into ArangoDB using the command
```bash
bash ./code/load_arangodb.sh
```
Once the process is finished you will be displayed the time taken to complete the process.

Then navigate to the following [link](http://localhost:8529/_db/_system/_admin/aardvark/index.html#login) in a browser of your choice and enter the following credentials

- Username: ```root```
- Password: ```cs511grp9```

Then select the Database: ```_system```

You will be able to access the ArangoDB dashboard. Click on ```Queries``` in the left pane

You can now execute the queries given inside the ```Queries``` folder.

