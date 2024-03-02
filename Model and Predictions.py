
# Imports and .apply()
import json
import os
import re
import asyncio
import nest_asyncio
nest_asyncio.apply()
from datetime import datetime
from pyensign.ensign import authenticate, publisher, subscriber
from datetime import datetime
from collections import Counter
from river import stream, preprocessing,ensemble
from river import tree, compose, metrics

# Retrieve Ensign Credentials and Initialize Arrays
ENSIGN_CREDS_PATH ='Ensign_Creds.json'

Metro_arr = []
count_arr = []


# Class to Encode Variables
class CustomLabelEncoder:
    def __init__(self):
        # Initialize dictionaries and a counter to manage encoding and decoding
        self.label_to_int = {}  # Dictionary to map labels to integer values
        self.int_to_label = {}  # Dictionary to map integer values back to labels
        self.next_int = 0  # Counter to track the next available integer for encoding

    def learn_one(self, label):
        # Learn and encode a label if not already encountered
        if label not in self.label_to_int:
            # Assign the next available integer to the label
            self.label_to_int[label] = self.next_int
            # Map the integer back to the label
            self.int_to_label[self.next_int] = label
            # Increment the counter for the next available integer
            self.next_int += 1
        return self  # Return the encoder object for method chaining

    def transform_one(self, label):
        # Encode a label to its corresponding integer
        return self.label_to_int[label]

    def inverse_transform(self, int_value):
        # Decode an integer back to its original label
        return self.int_to_label[int_value]

# Initialize label_encoder, model, scaler, and metric
label_encoder = CustomLabelEncoder()
model = tree.HoeffdingTreeClassifier()
scaler = preprocessing.StandardScaler()
metric = metrics.Accuracy()


# Initialize some lists for record keeping

accuracylist=[]
Correct_Incorrect_List=[]
truelist=[]
predlist=[]
truelist_decoded=[]
predlist_decoded=[]

# Initialize OneHotEncoder
oh_encoder = preprocessing.OneHotEncoder(drop_zeros=True)


def W_M_Model1(event_norm, label_encoder, model, scaler, metric, oh_encoder):
    currentdict = event_norm

    # Grab the current dictionary's target value and add it to its own list
    current_target = currentdict['description simplified']
    yi_encoded = label_encoder.learn_one(current_target).transform_one(current_target)
    yi = yi_encoded

    # Remove non-predictor variables from the dictionary
    del(currentdict['date_updated'])
    del(currentdict['routes_affected'])
    del(currentdict['summary'])
    del(currentdict['description simplified'])
    del(currentdict['incident_id'])
    del(currentdict['incident_type'])
    del(currentdict['description'])

    # Define a feature preprocessing pipeline
    feature_preprocessing = compose.Pipeline(
        compose.SelectType(str) | oh_encoder,  # Select and apply OneHotEncoder to string (categorical) features
        compose.SelectType(float) | preprocessing.StandardScaler()  # Select and standardize float (numerical) features
)

    # Apply feature preprocessing
    xi = feature_preprocessing.learn_one(currentdict).transform_one(currentdict)

    # Make a prediction and update the metric
    y_pred = model.predict_one(xi)

    metric.update(yi, y_pred)

    # Update the Model
    model.learn_one(xi,yi)

    # Decode the encoded values using the label encoder
    decoded_yi = label_encoder.inverse_transform(yi)
    try:
        decoded_y_pred = label_encoder.inverse_transform(y_pred)
    except:
        decoded_y_pred="ErrDecode"
 

    # Print Statements and Record Keeping
    
    # Try: Excepts to handle errors and prevent code from stopping completely.
    print(f'yi (decoded): {decoded_yi}', f'y_pred (decoded): {decoded_y_pred}')
    print(f"Accuracy: {metric.get():.3f}")
    accuracylist.append(metric.get())
    truelist.append(yi)
    truelist_decoded.append(decoded_yi)
    try:
        predlist.append(y_pred)
        predlist_decoded.append(decoded_y_pred)
    except:
        print("error_1")
        predlist.append("ERR")
        predlist_decoded.append("ERR")


    # Track prediction streak
    if decoded_yi == decoded_y_pred:
        Correct_Incorrect_List.append(1)

    else:
        Correct_Incorrect_List.append(0)

'''
get_updatesMetro runs and then waits until a metro event occures in the API. Once it occurs the print_updateMetro function is called
to transform the event data inot a json object which is then pushed to the metro array for storage
'''
@authenticate(cred_path=ENSIGN_CREDS_PATH)
@subscriber("Merged_DC_Data_Final")
async def get_updatesMetro(updates, label_encoder, model, scaler, metric):
    print('Get Merged Update Test')
    async for event in updates:
        #print(event)
        await print_updateMetro(event, label_encoder, model, scaler, metric)
        await asyncio.sleep(1)

async def print_updateMetro(event, label_encoder, model, scaler, metric):
    event_norm = json.loads(event.data)
    if event_norm not in Metro_arr:
        Metro_arr.append(event_norm)
        W_M_Model1(event_norm, label_encoder, model, scaler, metric,oh_encoder)

# Run it!
asyncio.run(get_updatesMetro(label_encoder=label_encoder, model=model, scaler=scaler, metric=metric))


import pandas as pd
# Save recordkeeping
your_df = pd.DataFrame({'Column_Name': accuracylist})  # You can specify a column name here

# Save the Pandas DataFrame to a CSV file
your_df.to_csv("accuracy.csv", index=False)

your_df = pd.DataFrame({'Column_Name': Correct_Incorrect_List})  # You can specify a column name here

# Save the Pandas DataFrame to a CSV file
your_df.to_csv("correctIncorrect.csv", index=False)

import csv
file_names = ['accuracylist.csv', 'Correct_Incorrect_List.csv', 'truelist.csv', 'predlist.csv', 'truelist_decoded.csv', 'predlist_decoded.csv']

# Define the data to be written
data_to_write = [accuracylist, Correct_Incorrect_List, truelist, predlist, truelist_decoded, predlist_decoded]

# Iterate through the data and corresponding file names
for data, file_name in zip(data_to_write, file_names):
    with open(file_name, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        
        # Write the data
        writer.writerow(data)

print("CSV files have been created.")
