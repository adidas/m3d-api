M3D API
=======

![](static/images/m3d_logo.png)

**M3D** stands for _Metadata Driven Development_ and is a cloud and platform agnostic framework for the automated creation, management and governance of metadata and data flows from multiple source to multiple target systems. The main features and design goals of M3D are:

*   Cloud and platform agnostic
*   Enforcement global data model including speaking names and business objects
*   Governance by conventions instead of maintaining state and logic
*   Lightweight and easy to use
*   Flexible development of new features
*   Stateless execution with minimal external dependencies
*   Enable self-service
*   Possibility to extend to multiple destination systems (currently AWS EMR)

M3D consists of two components. m3d-api which we're providing in this repo, and [m3d-engine](https://github.com/adidas/m3d-engine) containing the main logic using Apache Spark.

### Use cases

M3D can be used for:

*  Creation of data lake environments
*  Management and governance of metadata
*  Data flows from multiple sources
*  Data flows to multiple target systems
*  Algorithms as data frame transformations

adidas is not responsible for the usage of this software for different purposes that the ones described in the use cases.

### M3D Architecture

M3D is based on a layered architecture, using [AWS S3](https://aws.amazon.com/s3/) buckets as storage and Spark/Scala for processing.
Using the M3D api you can create data lake environments in a reproducible way.
These are the layers defined in the M3D architecture:

*   At the lowest level we have the **inbound layer**, where raw data is uploaded by source systems. The format of the source data is not fixed and a number of formats are supported by M3D. Only this layer is accessible by external non-M3D governed systems.
*   On top of the inbound layer, we have the **landing layer**, in which archived raw data from the inbound layer is stored together with the metadata that is used for further loading to the lake. It can be used for exploration on the raw files and for reprocessing but does not provide a Hive schema.
*   The next layer is the **lake layer** where data is persisted in parquet format for consumption by applications. This layer should be accessed using Hive. Also there are lake-to-lake algorithms that read from this layer and write to it.
*   The top layer is the **lake-out layer** which is a virtual layer for globally standardized semantic names.

Graphically, the architecture of M3D looks like this:

![](static/images/m3d_layers.svg)


### AWS Prerequisites for Out of the Box Usage

*   You will need to create three S3 buckets: **inbound**, **landing**, **lake** and **application**. The latter will contain the jar artifact from the M3D-engine.
*   An account for managing clusters in the AWS console.
*   A host machine with internet access.
*   An access key with permissions to write to the specified buckets and to create/delete EMR clusters.
*   Databases for **landing**, **lake** and **lake\_out**.

### Setup and Deployment: The Easy Way

The quickest way to get started with M3D API is to use GUI installer available for different platforms (Windows/Linux/Mac). With the GUI installer you can setup m3d-api and m3d-engine on a remote host (or localhost if you have an unix based system) and load local tables into AWS an EMR environment right out the box.
This of course requires an active AWS account that can be created by visiting this [link](https://aws.amazon.com).
If you already have an AWS account, make sure to get your access key and secret access key for successful installation and deployment of environment in EMR.
You can go to this [repository](https://github.com/adidas/m3d-opensource-installer.git) to build the installer the UI for your preferred OS.

After the installation completed the final steps of the GUI installer are:

*   Display sample data to be uploaded from on premises storage to AWS Cloud
*   Display the structure of tables in on premises database to be created in AWS cloud
*   Create the environment in the AWS Cloud
*   Upload the data to an S3 inbound bucket
*   Start the EMR cluster
*   Execute the Full Load spark algorithm contained in the m3d-engine to put data in the lake layer.
*   Shutdown EMR resources

### Setup and Deployment: Advanced Users

For advanced users, you may use [conda](https://conda.io) for installing M3D by entering the following command in your terminal: _conda install -c some-channel m3d-api_.

### Available API calls

*   **create\_table:** Creates a table in the AWS environment based on [TCONX Files](#faq).
*   **drop\_table:** Drops a table in the AWS environment. The files will remain in storage.
*   **truncate\_table**: Removes all files of a table from storage.
*   **create\_lake\_out\_view:** Executes an HQL statement to generate a view in the AWS environment.
*   **drop\_lake\_out\_view:** Removes a given view in the AWS environment.
*   **load\_table:** Loads a table in AWS from an specified source.
*   **run\_algorithm:** Executes an algorithm available in m3d-engine.
*   **create\_emr\_cluster:** Initializes an EMR cluster in AWS.
*   **delete\_emr\_cluster:** Terminates an EMR cluster in AWS.

### API Arguments

*   _\-function_: Name of the function to execute.
*   _\-config_: Location of the configuration json file. An example of configuration json file is provided below
    ```json
        {
            "emails": [
                "test@test.com"
            ],
            "dir_exec": "/tmp/",
            "python": {
                "main": "m3d_main.pyc",
                "base_package": "m3d"
            },
            "subdir_projects": {
                "m3d_engine": "m3d-engine/target/scala-2.11/",
                "m3d_api": "m3d-api/"
            },
            "tags": {
                "full_load": "full_load",
                "delta_load": "delta_load",
                "append_load": "append_load",
                "table_suffix_stage": "_stg1",
                "table_suffix_swap": "_swap",
                "config": "config",
                "system": "system",
                "algorithm": "algorithm",
                "table": "table",
                "view": "view",
                "upload": "upload",
                "pushdown": "pushdown",
                "aws": "aws",
                "hdfs": "hdfs",
                "file": "file"
            },
            "data_dict_delimiter": "|"
        }
    ```
*   _\-cluster\_mode_: Specifies whether the function should execute in a cluster or on a single node.
*   _\-destination\_system_: Name of the system to which data will be loaded.
*   _\-destination\_database_: Name of the destination database.
*   _\-destination\_environment_: Name of the different environments (test, dev, preprod, prod, etc.)
*   _\-destination\_table_: Name of the table in the destination\_database of the destination\_system where data will be written to.
*   _\-algorithm\_instance_: Name of the algorithm from m3d-engine to be executed
*   _\-load\_type_: Type of the load algorithm to be executed (FullLoad, DeltaLoad, or AppendLoad).
*   _\-ext\_params_: parameters in JSON format expected by an algorithm in M3D-engine.
*   _\-spark\_params_: Spark parameters in JSON format.
*   _\-core\_instance\_count_: Number of executor nodes in the EMR cluster.
*   _\-core\_instance\_type_: AWS node instance type for each executor node in the EMR cluster.
*   _\-master\_instance\_type_: AWS node instance type for the master node in the EMR cluster.
*   _\-emr\_version_: Version of EMR to use in for EMR clusters.

Not all arguments are mandatory for API calls. Please check the source code to identify required parameters for the API you would like to use.

### Example Use Case: Loading Data into AWS Environment

As an example of M3D capabilities, we provide an example that will load data from data files into AWS. **Prerequisites:** `cd` into your working directory where you have m3d-api and m3d-engine copied, whether it was from conda or from the GUI installer. For M3D-engine, you will need the compiled jar or build it manually with SBT.

Before you proceed, make sure you have everything in the prerequisites section completed and that entries in the config.json file has been made to match your setup. Also, make sure the relevant information is in the tconx file, such as column names, lake table name, destination database, etc. Note that for the example below, destination\_database is set to emr\_db, destination\_system is emr and destination\_environment is test. For table\_name, we use test\_table. Database name in M3D layers, should match the names you defined in the prerequisites section.

The steps are the following:

*   Upload a csv file containing the data to be uploaded to the lake. You can use aws cli for placing the file in inbound bucket.
    ```
    aws s3 cp s3://your-inbound-bucket/test/data.csv
    ```
    
*   Create an instance of EMR cluster
    ```
    python m3d_main.py -function create_emr_cluster \
        -core_instance_type m4.large \
        -master_instance_type m4.large \
        -core_instance_count 3 \
        -destination_system emr \
        -destination_database emr_database \
        -destination_environment test \
        -config /relative/to/m3d-api/config/m3d/config.json \
        -emr_version emr-5.23.0
    ```
    
*   Create the environment in AWS by invoking the API create\_table
    ```
    python m3d_main.py -function create_table \
        -config /relative/to/m3d-api/config/m3d/config.json \
        -destination_system emr \
        -destination_database emr_database \
        -destination_environment test \
        -destination_table table_name 
        -cluster_mode False \
        -emr_cluster_id id-of-started-cluster
    ```
    
*   Trigger the FullLoad algorithm in M3D-engine to load from inbound into lake layer.
    ```python m3d_main.py -function load_table \
        -config /relative/to/m3d-api/config/m3d/config.json \
        -cluster_mode False \
        -destination_system emr \
        -destination_database emr_database \
        -destination_environment test \
        -destination_table table_name \
        -load_type FullLoad \
        -emr_cluster_id id-of-started-cluster
    ```
    
*   **OPTIONAL:** Shutdown EMR cluster - Normally, after completion of a load job, you will stop the current EMR cluster, but if you would like to connect to your cluster after the loading job is completed, you can avoid executing this final API call to keep the cluster running. Afterwards, you can open HUE to query data via hive in the running EMR cluster. This can be done by connecting to the master instance if it was configured as suggested in this [guide](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html).
    ```
    python m3d_main.py -function delete_emr_cluster \
        -config /relative/to/m3d-api/config/m3d/config.json \
        -cluster_mode False \
        -destination_system emr \
        -destination_database emr_database \
        -destination_environment test \
        -emr_cluster_id id-of-started-cluster
    ```

### License and Software Information

© adidas AG

adidas AG publishes this software and accompanied documentation (if any) subject to the terms of the Apache 2.0 license with the aim of helping the community with our tools and libraries which we think can be also useful for other people. You will find a copy of the Apache 2.0 license in the root folder of this package. All rights not explicitly granted to you under the Apache 2.0 license remain the sole and exclusive property of adidas AG.

NOTICE: The software has been designed solely for the purpose of automated creation, management and governance of metadata and data flows. The software is NOT designed, tested or verified for productive use whatsoever, nor or for any use related to high risk environments, such as health care, highly or fully autonomous driving, power plants, or other critical infrastructures or services.

If you want to contact adidas regarding the software, you can mail us at _software.engineering@adidas.com_.

For further information open the [adidas terms and conditions](https://github.com/adidas/adidas-contribution-guidelines/wiki/Terms-and-conditions) page.


#### License

[Apache 2.0](LICENSE)


### FAQ

*   **What is a TCONX file?** It is a JSON file containing the definition of a table to be created in an Hadoop environment. Entries in the file include destination database, table name in lake, table columns, name of columns in the different M3D layers. For an example of what a TCONX file looks like, you can take a look at the [samples](https://github.com/adidas/m3d-api/tree/master/samples) subdirectory in this repo. It is important to note that the parameters mentioned above (table name, environment, etc) are part of the TCONX file naming convention. In samples/tconx-(emr)-(emr\_database)-(test)-(prefix)\_(table\_name).json, we can find the following parts in parenthesis:
    *   emr - _this is the destination system_
    *   emr\_database - _this is the destination database_
    *   test - _this is the destination environment_
    *   prefix - _this is the name of the source system generating the data_
    *   table\_name - _name of the table which the tconx file was generated for._
