## Running examples locally
### Using Config File
##### 1. Download the repo
##### 2. Copy the examples folder(found in root) to /tmp directory in your machine
##### 3. Run the following commmand
### 
    spark-submit \
    --master local \
    --class com.firemindlabs.operation.FeatureGenerator \
    file:///tmp/examples/Featurer-assembly-1.0-SNAPSHOT.jar \
    --config-path "/tmp/examples/config_test.json"
    
### Using Parameters directly using commandline
##### Alternatively you can run the application by passing the parameters directly in the commandline as shown below;
###
    spark-submit \
      --class com.firemindlabs.operation.FeatureGenerator \
      --master local \
      file:///tmp/examples/Featurer-assembly-1.0-SNAPSHOT.jar \
      --static-features "status:int,balance:int" \
      --force-categorical "null" \
      --dynamic-features "sum,min,max,stddev" \
      --labels-path "/tmp/examples/label_test.csv" \
      --eavt-path "/tmp/examples/eavt_test.csv" \
      --window "1,2," \
      --null-replacement "null" \
      --output-path "/tmp/featurer-output"