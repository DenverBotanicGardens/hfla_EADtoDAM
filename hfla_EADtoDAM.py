import pandas as pd
import xml.etree.ElementTree as etree
import re
import csv
import os

#THIS SCRIPT PULLDATA FROM AN EAD XML REPOSITORY AND FORMATS IT INTO CSV FOR UPLOAD INTO THE DBG CHORUS DAM. IT ALSO PULLS INFORMATION FROM THE CONATINER TEMPLATE SO THAT URIS TO EACH RESOURCE CAN BE SUPPLIED.

#Using Archive Space, export both the EAD xml archive and the Container Template csv.
#------------------------------------------------------------------------------------------------------------------------------#
#SET the EAD xml archive to be parsed here:
EADfile = 'HFLA_001_20230718_160733_UTC__ead.xml'                                                                                    #
#SET the Container Template CSV to be parsed here:                                                                             #
asContainerTemplateCSV = "1689703332.csv"                                                                                 #
#------------------------------------------------------------------------------------------------------------------------------#

ElementTree = etree.parse(EADfile)                                                             # 
root = ElementTree.getroot()

result_list = []
csv_columns = ['unittitle', 'container', 'persname', 'subject', 'unitdate', 'abstract']
csv_file = 'eadOutput.csv'

#for did_headings in root.findall(".//{urn:isbn:1-931666-22-9}did"):
for did_headings in root.findall(".//{urn:isbn:1-931666-22-9}c"):
	dictionary = {
	"unittitle" : "",
	"container" : "",
	"persname" : "",
	"subject" : "",
	"unitdate" : "",
	"abstract" : ""
	}

	for unittitle in did_headings.findall(".//{urn:isbn:1-931666-22-9}unittitle"):
		dictionary['unittitle'] = dictionary['unittitle'] + unittitle.text + ' | ' + did_headings.attrib['id']

	for container in did_headings.findall(".//{urn:isbn:1-931666-22-9}container"):
		dictionary['container'] = dictionary['container'] + container.text
	
	for persname in did_headings.findall(".//{urn:isbn:1-931666-22-9}persname"):
		dictionary['persname'] = dictionary['persname'] + persname.text
	
	for subject in did_headings.findall(".//{urn:isbn:1-931666-22-9}subject"):
		dictionary['subject'] = dictionary['subject'] +'"'+ subject.text + '"'+','
	
	for unitdate in did_headings.findall(".//{urn:isbn:1-931666-22-9}unitdate"):
		dictionary['unitdate'] = dictionary['unitdate'] + unitdate.text

	for abstract in did_headings.findall(".//{urn:isbn:1-931666-22-9}abstract"):
		dictionary['abstract'] = dictionary['abstract'] + abstract.text	
		
	
	result_list.append(dictionary)
	#print(result_list)

with open(csv_file, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
    writer.writeheader()
    for data in result_list:
    	writer.writerow(data)
   
#------------------------------------------------------------------------------------------------------------------------------------

#Function to get reference ID out of unititle field and put it in it's own row. Also gets rid of trailing commas in subject column.
def split_and_append(row):
    split_values = row['unittitle'].split('|')
    row['unittitle'] = split_values[0]
    row['Ref ID'] = split_values[1].replace(' aspace_','')
    return row

# Read the CSV file
input_file = "eadOutput.csv"
df = pd.read_csv(input_file)

# Apply the split_and_append function to each row
df = df.apply(split_and_append, axis=1)
df['subject'] = df['subject'].str.rstrip(',')

# Save the modified DataFrame back to CSV
output_file = "eadOutput_formatted.csv"
df.to_csv(output_file, index=False)

#------------------------------------------------------------------------------------------------------------------------------------
#Remove the first 23 rows from the container template

def remove_headers_and_rows(csv_file, rows_to_remove=3):
    # Read the CSV file skipping the first 'rows_to_remove' rows (headers + 2 additional rows)
    df = pd.read_csv(csv_file, skiprows=rows_to_remove)
    
    # Write the updated data to a new CSV file
    updated_csv_file = 'formatted_asContainerTemplateCSV.csv'
    df.to_csv(updated_csv_file, index=False)
    
    return updated_csv_file

# Replace 'your_file.csv' with the path to your actual CSV file
csv_file_path = asContainerTemplateCSV

# Specify the number of rows to remove (headers + 2 additional rows in this case)
rows_to_remove = 3

# Call the function to remove the headers and additional rows
new_csv_file = remove_headers_and_rows(csv_file_path, rows_to_remove)


#---------------------------------------------------------------------------------------------------------------------
# Merge data from EAD and Container Template to create URI for each resource

# Step 1: Read the data from both CSV files
formattedEADcsv = "eadOutput_formatted.csv"

data_df = pd.read_csv(formattedEADcsv)
additional_data_df = pd.read_csv('formatted_asContainerTemplateCSV.csv')

# Step 2: Identify the common identifier column in both CSVs (Assuming "ID" is the common identifier)
common_identifier = "Ref ID"

# Step 3: Merge the data based on the common identifier
merged_df = pd.merge(data_df, additional_data_df, on=common_identifier, how="left")

# Step 4: Write the merged data to a new CSV file
merged_file = "mergedFiles.csv"
merged_df.to_csv(merged_file, index=False)

#print(f"Data merged successfully and saved to '{merged_file}'.")

#-------------------------------------------------------------------------------------------------------------------------------
# Format the merged file to remove unwanted columns and create the URI's from the Archival Object ID

def remove_columns_from_csv(csv_file, columns_to_remove):
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Remove the specified columns
    df = df.drop(columns=columns_to_remove)
    
    # Write the updated data to a new CSV file
    updated_csv_file = "mergedFiles_columnsRemoved.csv"
    df.to_csv(updated_csv_file, index=False)
    
    return updated_csv_file

# Replace 'your_file.csv' with the path to your actual CSV file
csv_file_path = 'mergedFiles.csv'

# Specify the columns to remove (as a list)
columns_to_remove = ['unittitle', 'Field Name', 'Component ID', 'EAD ID', 'Instance Type', 'Top Container ID (existing top container, leave blank if creating new container)','Top Container Type','Top Container Indicator', 'Top container barcode', 'Container Profile ID', 'Child Type', 'Child Indicator', 'Child Barcode', 'Location ID']

# Call the function to remove the specified columns
new_csv_file = remove_columns_from_csv(csv_file_path, columns_to_remove)


#-------------------------------------------------------------------------------------
#add "https://archives.botanicgardens.org/repositories/2/archival_objects/" to the archival object id to make it a URI

# Function to add a string to a value in each row
def add_string_to_value(csv_file, column_name, string_to_add):
    updated_data = []

    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames
        for row in reader:
            row[column_name] = f"{string_to_add}{row[column_name]}"
            updated_data.append(row)

    with open(csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_data)

# Usage: Add " years old" to the "age" column in "data.csv"
add_string_to_value("mergedFiles_columnsRemoved.csv", "Archival Object ID", "https://archives.botanicgardens.org/repositories/2/archival_objects/")


#-------------------------------------------------------------------------------------------------------------------------------------------------------
#Rename column headers

def rename_headers(csv_file, new_headers):
    updated_data = []

    with open(csv_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames
        for row in reader:
            updated_row = {new_headers.get(header, header): value for header, value in row.items()}
            updated_data.append(updated_row)

    with open(csv_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=new_headers.values())
        writer.writeheader()
        writer.writerows(updated_data)

# Dictionary to map old header names to new header names
header_mapping = {
    "container": "Item ID",
    "persname": "Creator",
    "subject": "Keywords",
    "unitdate": "Publication Year",
    "abstract": "Caption",
    "Ref ID": "Archives Space Ref ID",
    "Archival Object ID": "references",
    "Title": "Title",
    "Resource Title": "Parent Resource",
    "Identifier": "Parent Resource ID"
}

# Usage: Rename column headers in "data.csv"
rename_headers("mergedFiles_columnsRemoved.csv", header_mapping)


#-------------------------------------------------------------------------------------------------------------------------------------------------------
#Rename the final version of the data file

dataName = EADfile[0:8]
def rename_csv(old_filename, new_filename):
    try:
        os.rename(old_filename, new_filename)
        #print(f"CSV file '{old_filename}' renamed to '{new_filename}'.")
    except FileNotFoundError:
        print(f"Error: File '{old_filename}' not found.")
    except FileExistsError:
        print(f"Error: File '{new_filename}' already exists.")

# Usage: Rename 'old_data.csv' to 'new_data.csv'
rename_csv("mergedFiles_columnsRemoved.csv", dataName + "_metadataForDAM.csv")

#-------------------------------------------------------------------------------------------------------------------------------------------------------------
#Delete all the extra files created in the process

def delete_specified_files(directory_path, files_to_delete):
    try:
        for file_name in files_to_delete:
            # Construct the full path for each file
            file_path = os.path.join(directory_path, file_name)

            # Check if the path corresponds to a file (not a directory)
            if os.path.isfile(file_path):
                # Delete the file
                os.remove(file_path)
                #print(f"Deleted: {file_path}")
            else:
                print(f"File '{file_path}' not found.")

        #print("All specified files deleted successfully.")
    except FileNotFoundError:
        print(f"Error: Directory '{directory_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Usage: Delete specified files from the directory "example_directory"
directory = "."
files_to_delete = ["eadOutput_formatted.csv", "eadOutput.csv", "formatted_asContainerTemplateCSV.csv", "mergedFiles.csv"]
delete_specified_files(directory, files_to_delete)
